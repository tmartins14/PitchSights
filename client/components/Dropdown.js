import React from "react";

const Dropdown = ({ label, options, selectedOption, onSelectOption }) => {
  return (
    <div className="mb-4">
      <label htmlFor="dropdown" className="mr-2">
        {label}:
      </label>
      <select
        id="dropdown"
        className="border border-gray-400 rounded px-3 py-1"
        value={selectedOption}
        onChange={(e) => onSelectOption(e.target.value)}
      >
        {options.map((option, index) => (
          <option key={index} value={option.value}>
            {option.label}
          </option>
        ))}
      </select>
    </div>
  );
};

export default Dropdown;
