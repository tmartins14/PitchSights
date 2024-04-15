import React from "react";
import Header from "./Header";
import TacticsBoard from "./TacticsSimulation/TacticsBoard";

const Layout = ({ children }) => {
  return (
    <>
      <Header />
      <TacticsBoard />
      {/* <main>{children}</main> */}
    </>
  );
};

export default Layout;
