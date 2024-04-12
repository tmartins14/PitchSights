import React, { useState } from "react";
import { useSelector, useDispatch } from "react-redux";
import Link from "next/link";
import { logout, loginStart } from "../store/userSlice";

const Header = () => {
  const dispatch = useDispatch();
  const isLoggedIn = useSelector((state) => state.user.isLoggedIn);
  const isLoading = useSelector((state) => state.user.isLoading);

  const handleAuthClick = () => {
    if (isLoading) return;

    if (isLoggedIn) {
      dispatch(logout());
    } else {
      dispatch(loginStart());
    }

    // setIsLoggedIn(!isLoggedIn);
    console.log(isLoggedIn);
  };

  let renderedLogin;

  switch (isLoggedIn) {
    case true:
      renderedLogin = (
        <a
          href={`${process.env.server}/api/logout`}
          className="ml-4 cursor-pointer text-red-500"
          onClick={handleAuthClick}
        >
          Log Out
        </a>
      );
      break;
    case false:
      renderedLogin = (
        <a
          href={`${process.env.server}/auth/google`}
          className="ml-4 cursor-pointer text-blue-500"
          onClick={handleAuthClick}
        >
          Log In
        </a>
      );
      break;
    default:
      renderedLogin = <div className="ml-4 text-gray-500">Loading...</div>;
  }

  return (
    <header className="bg-white shadow-md py-4">
      <div className="container mx-auto flex justify-between items-center">
        <Link href="/" passHref>
          <span className="text-xl font-bold text-gray-800">PitchSights</span>
        </Link>
        {renderedLogin}
      </div>
    </header>
  );
};

export default Header;
