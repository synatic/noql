// Intercept clicks on the "Skill.md" nav tab and trigger a file download
// instead of navigating to the page. Uses capture phase so it fires before
// MkDocs Material's instant-navigation handler.
(function () {
    function isSkillLink(el) {
        var link = el.closest('a');
        if (!link) return null;
        var text = link.textContent.trim();
        if (text === 'Skill.md') return link;
        return null;
    }

    function triggerDownload() {
        // Resolve the asset URL relative to the site root
        var base = document.querySelector('base');
        var root = base ? base.href : '/';
        var assetUrl = root.replace(/\/$/, '') + '/assets/noql-skill.txt';

        fetch(assetUrl)
            .then(function (r) { return r.blob(); })
            .then(function (blob) {
                var url = URL.createObjectURL(blob);
                var a = document.createElement('a');
                a.href = url;
                a.download = 'noql-skill.md';
                document.body.appendChild(a);
                a.click();
                document.body.removeChild(a);
                URL.revokeObjectURL(url);
            })
            .catch(function () {
                // Fallback: let the browser navigate to the raw asset
                window.location.href = assetUrl;
            });
    }

    document.addEventListener(
        'click',
        function (e) {
            var link = isSkillLink(e.target);
            if (link) {
                e.preventDefault();
                e.stopImmediatePropagation();
                triggerDownload();
            }
        },
        true // capture phase — fires before Material's navigation handler
    );
})();
