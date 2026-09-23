// Shared hamburger-drawer toggle for the topbar nav, used across all pages.
// Expects #navToggle (the button) and #navLinks (the collapsible nav).
(function () {
  var toggle = document.getElementById('navToggle');
  var nav = document.getElementById('navLinks');
  if (!toggle || !nav) return;
  toggle.addEventListener('click', function () {
    var open = nav.classList.toggle('open');
    toggle.setAttribute('aria-expanded', open ? 'true' : 'false');
  });
})();
