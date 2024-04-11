import React, { useEffect, useState } from "react";
import Layout from "../components/Layout";

const index = () => {
  const [message, setMessage] = useState("Loading");

  useEffect(() => {
    fetch("http://localhost:8080/api/home")
      .then((response) => response.json())
      .then((data) => {
        console.log(data);
        setMessage(data.message);
      });
  }, []);

  return (
    <Layout>
      <div className="container mx-auto px-4">
        <h1 className="text-2x1 font-bold text-center my-4">{message}</h1>
      </div>
    </Layout>
  );
};

export default index;
