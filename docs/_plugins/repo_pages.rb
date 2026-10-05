# Publishes the repository's Markdown documents as site pages without moving
# or editing them: SPECIFICATIONS.md and friends stay where the Java sources
# cite them by path, and none of them needs Jekyll front matter, so each still
# reads cleanly on GitHub. The documents and their navigation entries are
# listed under `repo_pages` in _config.yml (paths relative to the repository
# root); `repo_files` lists repository files copied into the site unchanged.
module BeakGraph
  class RepoPages < Jekyll::Generator
    safe true

    # Just the Docs' in-page table of contents (kramdown's TOC, levels set by
    # kramdown.toc_levels), collapsed so long documents start with their text.
    TOC = <<~MD
      <details markdown="block">
        <summary>
          Contents
        </summary>
        {: .text-delta }
      - TOC
      {:toc}
      </details>
    MD

    def generate(site)
      root = File.expand_path("..", site.source)
      pages = site.config.fetch("repo_pages", []).map do |entry|
        source = entry.fetch("source")
        markdown = gfm_table_pipes(File.read(repo_path(root, source), mode: "r:bom|utf-8").gsub("\r\n", "\n"))
        page = Jekyll::PageWithoutAFile.new(site, site.source, "", File.basename(source))
        page.content = entry["toc"] ? with_toc(markdown, source) : markdown
        page.data.merge!(entry.reject { |key, _| %w[source toc].include?(key) })
        page.data["layout"] ||= "default"
        page.data["source_path"] = source
        # The documents are prose, not templates: a literal {{ or {% in a code
        # sample must not be evaluated as Liquid.
        page.data["render_with_liquid"] = false
        page
      end
      # Pages render in list order, and the theme's search index
      # (zzzz-search-data.json) is meant to render last, reading every other
      # page's finished HTML: appended, these pages would be indexed as Markdown.
      site.pages.unshift(*pages)
      site.config.fetch("repo_files", []).each do |file|
        repo_path(root, file)
        dir = File.dirname(file)
        site.static_files << Jekyll::StaticFile.new(site, root, dir == "." ? "" : dir, File.basename(file))
      end
    end

    private

    def repo_path(root, relative)
      path = File.join(root, relative)
      return path if File.file?(path)

      raise Jekyll::Errors::FatalException, "_config.yml names #{relative}, which is not in the repository"
    end

    # GitHub needs a pipe inside a table cell's code span escaped (`a \| b`)
    # and drops the backslash; kramdown keeps a bare pipe in its code span and
    # cell, and would print the backslash. Fenced code is left alone.
    def gfm_table_pipes(markdown)
      fenced = false
      markdown.lines.map do |line|
        fenced = !fenced if line.match?(/\A {0,3}(```|~~~)/)
        next line if fenced || !line.match?(/\A\s*\|/)

        line.gsub(/`[^`]*`/) { |code| code.gsub("\\|", "|") }
      end.join
    end

    # Puts the table of contents under the document's title, which is kept
    # out of the contents itself.
    def with_toc(markdown, source)
      return markdown.sub(/\A(# [^\n]*\n)/) { "#{$1}{: .no_toc }\n\n#{TOC}\n" } if markdown.start_with?("# ")

      Jekyll.logger.warn "repo_pages:", "#{source} does not start with a '# ' title; no table of contents added"
      markdown
    end
  end
end
