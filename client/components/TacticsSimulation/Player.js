import React from "react";

const Player = ({ player }) => {
  return (
    <div
      className="absolute rounded-full bg-blue-500 text-white w-8 h-8 flex items-center justify-center"
      style={{ top: player.position.y, left: player.position.x }}
    >
      <span className="text-xs">{player.positionNumber}</span>
    </div>
  );
};

export default Player;
