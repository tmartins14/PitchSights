import React, { useState } from "react";
import Button from "./Button";
import Link from "next/link";

const Header = () => {
  const [isLoggedIn, setIsLoggedIn] = useState(false);
  const [isLoading, setIsLoading] = useState(false);

  const handleAuthClick = () => {
    setIsLoggedIn(!isLoggedIn);
  };
  return (
    <header className="bg-white shadow-md py-4">
      <div className="container mx-auto flex justify-between items-center">
        <Link href="/" passHref>
          <span className="text-xl font-bold text-gray-800">PitchSights</span>
        </Link>
        <Button
          onClick={handleAuthClick}
          primary={!isLoggedIn}
          danger={isLoggedIn}
          loading={isLoading}
          className="ml-4"
        >
          {isLoggedIn ? "Log Out" : "Log In"}
        </Button>
      </div>
    </header>
  );
};

export default Header;
