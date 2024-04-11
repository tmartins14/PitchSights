// Function to introduce a delay
const delay = () => {
  return new Promise((resolve) => {
    setTimeout(resolve, 1000 * 60); // Delay for 1 minute
  });
};

module.exports = delay;
