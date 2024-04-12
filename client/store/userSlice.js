import { createSlice } from "@reduxjs/toolkit";

export const userSlice = createSlice({
  name: "user",
  initialState: {
    isLoggedIn: false,
    isLoading: false,
  },
  reducers: {
    loginStart: (state) => {
      state.isLoading = true;
    },
    loginSuccess: (state) => {
      state.isLoggedIn = true;
      state.isLoading = false;
    },
    logout: (state) => {
      state.isLoggedIn = false;
      state.isLoading = false;
    },
  },
});

export const { loginStart, loginSuccess, logout } = userSlice.actions;

export default userSlice.reducer;
