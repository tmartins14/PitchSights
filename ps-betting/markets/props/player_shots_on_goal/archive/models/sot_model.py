from typing import Tuple, List, Optional
import warnings
import numpy as np
import pandas as pd

from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import RandomizedSearchCV, GroupKFold
from sklearn.metrics import log_loss, roc_auc_score

import xgboost as xgb

# Optional resampling (pip install imbalanced-learn)
try:
    from imblearn.over_sampling import SMOTE
    from imblearn.under_sampling import RandomUnderSampler
    IMB_OK = True
except Exception:
    IMB_OK = False


def train_and_predict_model(
    df_cleaned: pd.DataFrame,
    feature_cols: List[str],
    gameweek: int,
    model_type: str = "xgboost",        # "xgboost" | "random_forest"
    market: str = "player_shots_on_target",
    bet_type: str = "bet",
    # ---- imbalance & tuning knobs ----
    handle_imbalance: str = "auto",     # "auto" | "none"
    resample: Optional[str] = None,     # None | "smote" | "under"
    tune: bool = True,
    n_iter: int = 25,
    cv_splits: int = 4,
    random_state: int = 42,
    # ---- prediction threshold ----
    predict_threshold: Optional[float] = None,  # None => dynamic (break-even); float => fixed threshold
    threshold_buffer: Optional[float] = None,
    min_ev: Optional[float] = None
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Train on all weeks before `gameweek`, test on `gameweek`, and return predictions + feature importance.
    Adds class-imbalance handling and optional hyperparameter tuning with chronological CV.
    Uses xgboost.train for early stopping (version-agnostic).

    Prediction rule:
      - If `predict_threshold` is None (default): dynamic threshold per row:
            predicted = probability > break_even_prob
        where break_even_prob = implied_prob (if present) else 1 / bet_odds.
      - If `predict_threshold` is a float: fixed threshold:
            predicted = probability >= predict_threshold
    """

    # ---------------- Basic split (leak-free) ----------------
    X = df_cleaned[feature_cols]
    y = df_cleaned["target_hit_line"].astype(int)

    train_idx = df_cleaned["gameweek_number"] < gameweek
    test_idx  = df_cleaned["gameweek_number"] == gameweek

    if not test_idx.any():
        raise ValueError(f"No data found for gameweek {gameweek}")

    # Validation slice for early stopping = last train week
    last_train_week = df_cleaned.loc[train_idx, "gameweek_number"].max()
    val_mask = train_idx & (df_cleaned["gameweek_number"] == last_train_week)
    base_train_mask = train_idx & (~val_mask)  # remaining pre-test rows

    X_tr, y_tr = X.loc[base_train_mask], y.loc[base_train_mask]
    X_val, y_val = X.loc[val_mask], y.loc[val_mask]

    # ---------------- Optional resampling on TRAIN (no leakage) ----------------
    if resample is not None:
        if not IMB_OK:
            raise ImportError("imblearn is not installed but resample was requested.")
        if resample == "smote":
            sampler = SMOTE(random_state=random_state)
        elif resample == "under":
            sampler = RandomUnderSampler(random_state=random_state)
        else:
            raise ValueError("resample must be None, 'smote', or 'under'")
        X_tr, y_tr = sampler.fit_resample(X_tr, y_tr)

    # ---------------- Build base model (imbalance-aware) ----------------
    booster = None  # will hold trained xgb Booster if used

    if model_type == "random_forest":
        rf_kwargs = dict(
            n_estimators=400,
            max_depth=None,
            min_samples_split=2,
            min_samples_leaf=1,
            max_features="sqrt",
            n_jobs=-1,
            random_state=random_state
        )
        if handle_imbalance == "auto":
            rf_kwargs["class_weight"] = "balanced_subsample"
        model = RandomForestClassifier(**rf_kwargs)

    elif model_type == "xgboost":
        # scale_pos_weight computed on training subset (post-resample if used)
        spw = None
        if handle_imbalance == "auto":
            pos = int(y_tr.sum())
            neg = int(y_tr.shape[0] - pos)
            spw = (neg / max(pos, 1)) if pos > 0 else 1.0

        xgb_kwargs = dict(
            objective="binary:logistic",
            eval_metric="logloss",
            n_estimators=600,
            max_depth=4,
            learning_rate=0.05,
            subsample=0.9,
            colsample_bytree=0.9,
            min_child_weight=1,
            reg_lambda=1.0,
            random_state=random_state,
            tree_method="hist",
        )
        if spw is not None:
            xgb_kwargs["scale_pos_weight"] = spw

        # Use sklearn wrapper for tuning; final fit switches to Booster API
        model = xgb.XGBClassifier(**xgb_kwargs)
    else:
        raise ValueError("Invalid model_type. Choose 'xgboost' or 'random_forest'.")

    # ---------------- Tuning with GroupKFold on ALL train weeks ----------------
    groups_all_train = df_cleaned.loc[train_idx, "gameweek_number"].values
    unique_groups = np.unique(groups_all_train)
    n_groups = unique_groups.size

    if tune:
        if n_groups < 2:
            warnings.warn(f"Not enough groups for tuning (found {n_groups}). Skipping tuning.")
            tune = False
        elif cv_splits > n_groups:
            warnings.warn(
                f"cv_splits={cv_splits} > n_groups={n_groups}. Reducing cv_splits to {n_groups}."
            )
            cv_splits = n_groups

    if tune:
        gkf = GroupKFold(n_splits=cv_splits)
        if model_type == "random_forest":
            param_dist = {
                "n_estimators":        [300, 500, 800, 1000],
                "max_depth":           [None, 6, 10, 14, 18],
                "min_samples_split":   [2, 5, 10],
                "min_samples_leaf":    [1, 2, 4],
                "max_features":        ["sqrt", "log2", 0.5, 0.7],
            }
            if handle_imbalance == "auto":
                param_dist["class_weight"] = ["balanced_subsample"]

            tuner = RandomizedSearchCV(
                estimator=model,
                param_distributions=param_dist,
                n_iter=n_iter,
                scoring="roc_auc",
                cv=gkf.split(X.loc[train_idx], y.loc[train_idx], groups_all_train),
                n_jobs=-1,
                random_state=random_state,
                verbose=0
            )
            tuner.fit(X.loc[train_idx], y.loc[train_idx])
            model = tuner.best_estimator_

        else:  # xgboost
            param_dist = {
                "n_estimators":      [400, 700, 1000, 1400],
                "max_depth":         [3, 4, 5],
                "learning_rate":     [0.02, 0.03, 0.05, 0.07],
                "subsample":         [0.7, 0.85, 1.0],
                "colsample_bytree":  [0.7, 0.85, 1.0],
                "min_child_weight":  [1, 3, 5],
                "reg_lambda":        [0.5, 1.0, 1.5, 2.5],
                        }

 
            tuner = RandomizedSearchCV(
                estimator=model,
                param_distributions=param_dist,
                n_iter=n_iter,
                scoring="roc_auc",
                cv=gkf.split(X.loc[train_idx], y.loc[train_idx], groups_all_train),
                n_jobs=-1,
                random_state=random_state,
                verbose=0
            )
            # No early stopping inside CV
            tuner.fit(X.loc[train_idx], y.loc[train_idx])
            model = tuner.best_estimator_

    # ---------------- Final fit ----------------
    if model_type == "xgboost":
        # Use native Booster API for early stopping (version-agnostic)
        use_es = (X_val.shape[0] > 0) and (X.loc[train_idx].shape[0] > 0)
        if use_es:
            dtrain = xgb.DMatrix(X.loc[train_idx], label=y.loc[train_idx])
            dval   = xgb.DMatrix(X_val,              label=y_val)

            params = model.get_xgb_params()
            params["eval_metric"] = "auc"
            evals = [(dtrain, 'train'), (dval, 'eval')]

            booster = xgb.train(
                params=params,
                dtrain=dtrain,
                num_boost_round=1000,
                evals=evals,
                early_stopping_rounds=50,
                verbose_eval=False
            )
            model._Booster = booster  # attach trained booster back to sklearn wrapper
        else:
            warnings.warn("No separate validation slice available. Fitting XGBoost without early stopping.")
            model.fit(X.loc[train_idx], y.loc[train_idx], verbose=False)
    else:
        model.fit(X.loc[train_idx], y.loc[train_idx])

    # ---------------- Predict on test week ----------------
    if model_type == "xgboost" and getattr(model, "_Booster", None) is not None:
        dtest = xgb.DMatrix(X.loc[test_idx])
        best_iter = getattr(model._Booster, "best_iteration", None)
        if best_iter is not None:
            y_proba = model._Booster.predict(dtest, iteration_range=(0, best_iter + 1))
        else:
            y_proba = model._Booster.predict(dtest)
    else:
        y_proba = model.predict_proba(X.loc[test_idx])[:, 1]

    # ---------------- Outputs ----------------
    base_cols = [
        "player", "match_date", "gameweek_number", "bookmaker", "line", "minutes", "minutes_ma",
        f"{bet_type}_odds", "goals", "shots_on_target", "implied_prob"
    ]
    cols = [c for c in base_cols if c in df_cleaned.columns]

    results_df = df_cleaned.loc[test_idx, cols].copy()
    results_df["actual"] = y.loc[test_idx].values
    results_df["probability"] = y_proba

    # --- Break-even probability (market implied) ---
    if "implied_prob" in results_df.columns:
        results_df["break_even_prob"] = results_df["implied_prob"].astype(float)
    else:
        results_df["break_even_prob"] = 1.0 / results_df[f"{bet_type}_odds"].astype(float)

    # --- EV (for filtering & reporting) ---
    results_df["ev"] = results_df["probability"] * results_df[f"{bet_type}_odds"] - 1.0

    # --- Threshold logic ---
    if predict_threshold is None:
        # Dynamic: require model prob to beat market by a buffer
        dynamic_threshold = results_df["break_even_prob"] + float(threshold_buffer)
        pred_mask = results_df["probability"] >= dynamic_threshold
    else:
        # Fixed: use provided threshold (buffer ignored here)
        pred_mask = results_df["probability"] >= float(predict_threshold)

    # Optional EV floor to further tighten selection
    if min_ev is not None:
        pred_mask = pred_mask & (results_df["ev"] >= float(min_ev))

    results_df["predicted"] = pred_mask.astype(int)

    # Quick sanity metrics (whole test week)
    try:
        results_df["_logloss"] = log_loss(results_df["actual"], 
                                        np.clip(results_df["probability"], 1e-12, 1-1e-12))
        if results_df["actual"].nunique() > 1:
            results_df["_rocauc"] = roc_auc_score(results_df["actual"], results_df["probability"])
    except Exception:
        pass


    # Feature importances
    if model_type == "xgboost":
        try:
            booster = model.get_booster()
            fi_df = pd.DataFrame.from_dict(
                booster.get_score(importance_type="gain"),
                orient="index",
                columns=["importance"]
            ).reset_index().rename(columns={"index": "feature"})
            # If feature names exist, map them; otherwise keep f0,f1,...
            if booster.feature_names is not None and all(booster.feature_names):
                # Note: booster.feature_names order corresponds to training matrix order
                fi_df["feature"] = booster.feature_names
            fi_df = fi_df.sort_values("importance", ascending=False)
            fi_map = fi_df.set_index("feature")["importance"].to_dict()
            fi = [fi_map.get(feat, 0.0) for feat in feature_cols]
        except Exception as e:
            print(f"Error getting importances of features: {e}")
            fi = np.zeros(len(feature_cols))
    else:
        fi = getattr(model, "feature_importances_", np.zeros(len(feature_cols)))

    importance_df = pd.DataFrame({
        "feature": feature_cols,
        "importance": fi
    }).sort_values("importance", ascending=False)

    return results_df, importance_df
