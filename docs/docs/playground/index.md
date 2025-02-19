---
hide:
  - navigation
  - toc
  - footer
---

 <style>
  h1 {
    display: none;
  }
</style>


Enter a SQL statement below to see NoQL's output, as well as the equivalent Mongo Shell query, and Node.js code.

<div>
    <div class="admonition example">
          <div style="display: flex; align-items: center; gap:1rem;margin-bottom: 0.5rem;margin-top:0.5rem">
              <div style="display: flex; align-items: center; gap:0.25rem;">
                <input type="checkbox" id="force-aggregate" name="force-aggregate">
                <label for="force-aggregate">Always create an aggregate</label>
              </div>
              <div style="display: flex; align-items: center; gap:0.25rem;">
                <input type="checkbox" id="unwind-joins" name="unwind-joins">
                <label for="unwind-joins">Automatically Unwind Joins</label>
              </div>
              <div style="display: flex; align-items: center; gap:0.25rem;">
                <input type="checkbox" id="optimize-joins" name="unwind-joins">
                <label for="optimize-joins">Optimize Joins</label>
              </div>
          </div>
        <div class="playground-code-input" id="playground-sql-input">SELECT * FROM rockets</div>
    </div>
    <button class="md-button md-button--primary" id="submit-sql">Convert</button>
    <div id="playground-error-container" class="admonition failure" style="display:none">
        <p class="admonition-title">Error parsing SQL statement</p>
        <p id="playground-error-result"></p>
    </div>
</div>

<div class="result" id="playground-output-container" style="display:none">
    <div class="admonition success">
        <p class="admonition-title">Mongo Result</p>
        <div class="tabbed-set tabbed-alternate" data-tabs="1:3"
            style="--md-indicator-x: 0px; --md-indicator-width: 118px;">
            <input checked="checked" id="mongo-shell-output" name="__tabbed_5" type="radio">
            <input id="node-code-output" name="__tabbed_5" type="radio">
            <input id="noql-output" name="__tabbed_5" type="radio">
            <input id="noql-optimized-output" name="__tabbed_5" type="radio">
            <div class="tabbed-labels tabbed-labels--linked">
              <label for="mongo-shell-output">Mongo Shell Query</label>
              <label for="node-code-output">Node.js Code</label>
              <label for="noql-output">NoQL Output</label>
              <label for="noql-optimized-output">Optimized Pipeline</label>
            </div>
            <div class="tabbed-content">
                <div class="tabbed-block">
                    <div class="language-javascript highlight">
                        <pre><code class="playground-code-output language-javascript hljs" id="playground-mongo-result"></code></pre>
                    </div>
                </div>
                <div class="tabbed-block">
                    <div class="language-javascript highlight">
                        <pre><code class="playground-code-output language-javascript hljs" id="playground-node-result" ></code></pre>
                    </div>
                </div>
                <div class="tabbed-block">
                    <div class="language-javascript highlight">
                        <pre><code class="playground-code-output language-javascript hljs" id="playground-noql-result"></code></pre>
                    </div>
                </div>
                <div class="tabbed-block">
                    <div class="language-javascript highlight">
                        <pre><code class="playground-code-output language-javascript hljs" id="playground-noql-optimized-result"></code></pre>
                    </div>
                </div>
            </div>
            <div class="tabbed-control tabbed-control--prev" hidden=""><button class="tabbed-button" tabindex="-1"
                    aria-hidden="true"></button></div>
            <div class="tabbed-control tabbed-control--next" hidden=""><button class="tabbed-button" tabindex="-1"
                    aria-hidden="true"></button></div>
        </div>
    </div>
</div>
