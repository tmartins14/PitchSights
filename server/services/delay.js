// Function to introduce a delay
const delay = (accessLevel) => {
  // If accessLevel is 'trial', wait for 1 second; otherwise, proceed immediately
  return new Promise((resolve) => {
    if (accessLevel === "trial") {
      setTimeout(resolve, 1000); // Delay for 1 second
    } else {
      resolve(); // No delay
    }
  });
};

module.exports = delay;
