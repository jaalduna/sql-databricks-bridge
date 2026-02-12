import "@testing-library/jest-dom/vitest"

// Polyfill pointer capture methods missing from jsdom (needed by Radix UI)
if (typeof Element.prototype.hasPointerCapture !== "function") {
  Element.prototype.hasPointerCapture = () => false
}
if (typeof Element.prototype.setPointerCapture !== "function") {
  Element.prototype.setPointerCapture = () => {}
}
if (typeof Element.prototype.releasePointerCapture !== "function") {
  Element.prototype.releasePointerCapture = () => {}
}

// Polyfill scrollIntoView (used by Radix UI Select)
if (typeof Element.prototype.scrollIntoView !== "function") {
  Element.prototype.scrollIntoView = () => {}
}
