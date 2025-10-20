from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler
import xgboost as xgb
import pandas as pd
import numpy as np

def ags_model(df_cleaned, feature_cols, gameweek, model_type="xgboost"):
    """
    Train on all weeks before `gameweek`, test on `gameweek`, and return predictions + feature importance.
    """
    # Feature setup
    X = df_cleaned[feature_cols]
    y = df_cleaned['target_hit_line']

    # Define training and test indices
    train_idx = df_cleaned['gameweek_number'] < gameweek
    test_idx = df_cleaned['gameweek_number'] == gameweek

    if not test_idx.any():
        raise ValueError(f"No data found for gameweek {gameweek}")

    X_train, y_train = X[train_idx], y[train_idx]
    X_test, y_test = X[test_idx], y[test_idx]

    # Scale features
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    # Model selection
    if model_type == "random_forest":
        model = RandomForestClassifier(n_estimators=100, random_state=42)
    elif model_type == "xgboost":
        model = xgb.XGBClassifier(
            objective='binary:logistic',
            eval_metric='logloss',
            use_label_encoder=False,
            n_estimators=100,
            max_depth=3,
            learning_rate=0.1,
            random_state=42
        )
    else:
        raise ValueError("Invalid model_type. Choose 'random_forest' or 'xgboost'.")

    # Train model
    model.fit(X_train_scaled, y_train)
    y_pred = model.predict(X_test_scaled)
    y_proba = model.predict_proba(X_test_scaled)[:, 1]

    # Store predictions
    results_df = df_cleaned.loc[test_idx, [
        'player', 'match_date', 'gameweek_number','bookmaker', 'ags_odds', 'goals'
    ]].copy()
    results_df['actual'] = y_test.values
    results_df['predicted'] = y_pred
    results_df['probability'] = y_proba

    # Feature importance
    importance_df = pd.DataFrame({
        'feature': feature_cols,
        'importance': model.feature_importances_
    }).sort_values(by='importance', ascending=False)

    return results_df, importance_df
