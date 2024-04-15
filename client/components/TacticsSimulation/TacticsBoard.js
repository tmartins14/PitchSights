import React, { useState } from "react";
import Field from "./Field/Field";
import Dropdown from "../Dropdown";

const TacticsBoard = () => {
  const initialPlayers = [
    { positionNumber: 1, position: { x: 200, y: 200 } }, // Goalkeeper
    { positionNumber: 2, position: { x: 100, y: 100 } }, // Right Back
    { positionNumber: 3, position: { x: 150, y: 100 } }, // Left Back
    { positionNumber: 4, position: { x: 200, y: 100 } }, // Center Back
    { positionNumber: 5, position: { x: 250, y: 100 } }, // Center Back
    { positionNumber: 6, position: { x: 100, y: 200 } }, // Center Defensive Midfielder
    { positionNumber: 7, position: { x: 150, y: 200 } }, // Right Wing
    { positionNumber: 8, position: { x: 200, y: 150 } }, // Center Midfielder
    { positionNumber: 11, position: { x: 250, y: 200 } }, // Left Wing
    { positionNumber: 10, position: { x: 150, y: 250 } }, // Attacking Midfielder
    { positionNumber: 9, position: { x: 200, y: 250 } }, // Striker
  ];

  const formations = [
    { label: "4-4-2", value: "4-4-2" },
    { label: "4-3-3", value: "4-3-3" },
    { label: "3-5-2", value: "3-5-2" },
    { label: "3-4-3", value: "3-4-3" },
    { label: "4-2-3-1", value: "4-2-3-1" },
    { label: "4-1-4-1", value: "4-1-4-1" },
    { label: "5-3-2", value: "5-3-2" },
    { label: "4-5-1", value: "4-5-1" },
    { label: "4-3-2-1", value: "4-3-2-1" },
    { label: "4-4-1-1", value: "4-4-1-1" },
    // Add more formations as needed
  ];

  const fieldStyles = [
    { label: "Field", value: "Field" },
    { label: "White Board", value: "White Board" },
  ];

  const [players, setPlayers] = useState(initialPlayers);
  const [selectedFormation, setSelectedFormation] = useState(
    formations[0].value
  );
  const [selectedFieldStyle, setSelectedFieldStyle] = useState(fieldStyles[0]);

  const handleSelectFormation = (formation) => {
    setSelectedFormation(formation.value);
  };

  const handleSelectFieldStyle = (fieldStyle) => {
    setSelectedFieldStyle(fieldStyle);
  };

  return (
    <div className="container mx-auto p-4">
      <h1 className="text-2x1 font-bold mb-4">Football Tactics Simulator</h1>
      <div className="flex items-center mb-4">
        {" "}
        {/* Added flex container */}
        <div className="mr-4">
          {" "}
          {/* Added margin to space the dropdowns */}
          <Dropdown
            label="Formation"
            options={formations}
            selectedOption={selectedFormation}
            onSelectOption={handleSelectFormation}
          />
        </div>
        <div>
          <Dropdown
            label="Field Style"
            options={fieldStyles}
            selectedOption={selectedFieldStyle}
            onSelectOption={handleSelectFieldStyle}
          />
        </div>
      </div>
      <Field players={players} fieldStyle={selectedFieldStyle} />
    </div>
  );
};

export default TacticsBoard;
