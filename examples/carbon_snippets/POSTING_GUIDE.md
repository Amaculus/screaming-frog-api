# Carbon Snippet Posting Guide

## How to generate images

1. Run each script against a real `.dbseospider` crawl file
2. Copy the code + output into [carbon.sh](https://carbon.sh)
3. Recommended carbon.sh settings:
   - Theme: **Dracula** or **One Dark** (dark themes pop on social)
   - Language: **Python**
   - Font: **Fira Code** or **JetBrains Mono**
   - Padding: **32px**
   - Line numbers: **On**
   - Window controls: **On**

## Posting order (suggested narrative arc)

### Week 1: The Hook
| # | File | Caption | Hook |
|---|------|---------|------|
| 15 | `15_pip_install.py` | pip install screamingfrog. That's it. | Launch announcement |
| 01 | `01_hello_world.py` | Screaming Frog, meet Python. | First impression |
| 02 | `02_raw_sql.py` | SQL on Screaming Frog's Derby database. | The "wow" factor |

### Week 2: Core Features
| # | File | Caption | Hook |
|---|------|---------|------|
| 03 | `03_seo_audit.py` | A complete SEO audit. In Python. | Practical value |
| 14 | `14_gui_filters.py` | Every GUI filter. Now in Python. | SF users will recognize this |
| 10 | `10_page_data.py` | Every field SF captures. As a dict. | Data access story |

### Week 3: Power Features
| # | File | Caption | Hook |
|---|------|---------|------|
| 04 | `04_broken_link_graph.py` | Every 404 and every page linking to it. | Link graph is unique |
| 12 | `12_inlinks_outlinks.py` | The full link graph. Inlinks, outlinks, anchors. | Deep dive |
| 13 | `13_redirect_map.py` | Map every redirect. Find the chains. | Common pain point |

### Week 4: Advanced
| # | File | Caption | Hook |
|---|------|---------|------|
| 05 | `05_crawl_diff.py` | Crawl-over-crawl diffing. Catch regressions. | Unique feature |
| 07 | `07_any_format.py` | CSV, .seospider, .dbseospider. One API. | Flexibility story |
| 08 | `08_batch_analysis.py` | Audit 100 sites in seconds. | Scale story |

### Week 5: Infrastructure
| # | File | Caption | Hook |
|---|------|---------|------|
| 06 | `06_tab_discovery.py` | Every tab. Every filter. Discoverable. | Developer experience |
| 09 | `09_config_builder.py` | Build SF configs in Python. | Automation angle |
| 11 | `11_format_conversion.py` | .seospider in, .dbseospider out. | Pipeline story |

## Running the snippets

Most snippets expect a `./crawl.dbseospider` file in the current directory.
Adjust the path as needed or symlink your test crawl:

```bash
# Linux/Mac
ln -s /path/to/your/real-crawl.dbseospider ./crawl.dbseospider

# Windows
mklink crawl.dbseospider C:\path\to\your\real-crawl.dbseospider
```

For the diff snippet (#05), you need two crawl files:
```bash
ln -s /path/to/old-crawl.dbseospider ./crawl-before.dbseospider
ln -s /path/to/new-crawl.dbseospider ./crawl-after.dbseospider
```

For the batch snippet (#08), create a `./crawls/` directory with multiple `.dbseospider` files.

## Image format tips

- **Code + Output together**: Run the script, then paste the code at top and output below
  separated by a comment line like `# --- Output ---`
- **Keep it tight**: The snippets are designed to be 10-15 lines of code max
- **Real URLs matter**: Run against a real crawl - authentic URLs make it believable
- **Redact if needed**: Replace domain names with `example.com` if the crawl is private
