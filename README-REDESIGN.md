# BIM Infra · Premium Redesign Drop-in Pack

**Design System:** Blacksmith v2.0  ·  **Aesthetic:** Pure black + heritage gold  ·  **Status:** Non-destructive drop-in

This pack reskins the BIM Infra web portal (Flask/Jinja) and the BIM Nexus mobile app (Flutter) with a single, unified premium design language — black canvas, heritage gold accent, blueprint grid motifs, and a technical monospace rhythm that reflects BIM's millimeter-grade delivery culture.

Nothing in the original codebase has been overwritten. Every new artifact sits alongside the legacy code so you can A/B test, roll back instantly, or migrate page-by-page on your own schedule.

---

## 1. File Inventory

### Web Portal — Flask (`C:\Users\Kishan\BIM_CRM\`)

| Path | Purpose |
|---|---|
| `static/css/bim-v2.css` | Core design system. Tokens + Bootstrap overrides + custom components (KPI cards, chips, kanban, deal cards). Also exports legacy-alias variables so old base.html keeps working. |
| `templates_v2/base.html` | Drop-in replacement shell. Preserves every nav link, the chat widget include, role gating (`_team_role == 'admin'`), and Jinja url_for routes. Adds `head_extra` and `scripts` blocks. |
| `templates_v2/login.html` | Split-hero sign-in — blueprint-grid brand panel on the left, minimalist form on the right. POST action, remember checkbox, togglePw() JS all preserved. |
| `templates_v2/dashboard.html` | 6 KPI cards with SVG sparklines, Zoho Mail scanner row (preserves `/replies/sync-delivery`), invalid-leads table with `hunterFind()` / `hunterDomain()` handlers, pipeline health breakdown, Quick Launch grid. |
| `templates_v2/leads.html` | Filter bar (search/status/country/template), avatar-initial table, status & score chips, WhatsApp deep-link button, sequence progress bars. |
| `templates_v2/pipeline.html` | Kanban with per-stage accent color via CSS variable `--stage-color`. Drag-drop preserved (handleDragStart / handleDrop / moveCard → `/leads/{id}/move-stage`). |

### Mobile App — Flutter (`C:\Users\Kishan\BIM_Mobile\`)

| Path | Purpose |
|---|---|
| `lib/core/theme/app_theme_v2.dart` | Full Material 3 theme mirror of the web CSS. Exports `AppThemeV2.dark`, `BimColorsV2`, `BimTypographyV2`, `BimShadowsV2`, `BimRadiusV2`, plus reusable widgets `BimCard`, `BimChip` (with `BimChipTone`), `BimKpiCard` (with `BimDeltaTone`). |

---

## 2. Deploy the Flask Redesign

You have three options — pick the one that matches your comfort level.

### Option A — Flask template_folder swap (safest, reversible)

Tell Flask to serve the new templates instead of the old ones with a single line change.

```python
# app.py
app = Flask(
    __name__,
    template_folder="templates_v2",   # was: "templates"
    static_folder="static",
)
```

Restart the server. To roll back, change the value back to `"templates"`. Nothing was deleted, nothing was moved.

### Option B — Copy over (permanent migration)

When you're satisfied with the redesign, promote it:

```bash
cd C:\Users\Kishan\BIM_CRM
# Back up original templates first
cp -r templates templates_backup_pre_v2
# Promote v2 files
cp templates_v2\base.html      templates\base.html
cp templates_v2\login.html     templates\login.html
cp templates_v2\dashboard.html templates\dashboard.html
cp templates_v2\leads.html     templates\leads.html
cp templates_v2\pipeline.html  templates\pipeline.html
```

### Option C — Gradual migration (per-route)

Render specific pages from `templates_v2/` while keeping the rest on the legacy theme. In any Flask view:

```python
return render_template("../templates_v2/dashboard.html", **ctx)
```

This is useful when you want to demo the new dashboard to users without shipping a full migration.

### Required: load the new CSS

The new `bim-v2.css` is already linked inside `templates_v2/base.html` via:

```html
<link href="{{ url_for('static', filename='css/bim-v2.css') }}" rel="stylesheet">
```

No config change needed — the file lives in `static/css/` where Flask already serves static assets.

---

## 3. Deploy the Flutter Theme

Two-line change in `lib/main.dart`:

```dart
// 1. Swap the import
import 'core/theme/app_theme_v2.dart';      // was: 'core/theme/app_theme.dart'

// 2. Point MaterialApp at the new theme (inside build())
MaterialApp.router(
  title: 'BIM Nexus',
  debugShowCheckedModeBanner: false,
  theme:     AppThemeV2.dark,                // was: AppTheme.light
  darkTheme: AppThemeV2.dark,                // was: AppTheme.dark
  themeMode: ThemeMode.dark,                 // force dark — the design is black-first
  routerConfig: _router,
),
```

If you want to keep the light/dark toggle feature from `ThemeBloc`, leave the bloc in place and swap both `theme` and `darkTheme` to `AppThemeV2.dark` for now — a matching light variant can be added later.

Make sure `google_fonts: ^6.1.0` is already in `pubspec.yaml` (it is, per your existing Flutter stack). Run:

```bash
flutter pub get
flutter run
```

### Using the v2 widgets in your screens

```dart
import 'package:bim_nexus/core/theme/app_theme_v2.dart';

// KPI card with delta
BimKpiCard(
  label: 'Total Leads',
  value: '248',
  delta: '+12.4%',
  deltaTone: BimDeltaTone.up,
  sublabel: 'vs last week',
)

// Gold accent chip
BimChip(label: 'HOT', tone: BimChipTone.gold)

// Card with tap + optional blueprint background
BimCard(
  onTap: () => context.push('/leads/123'),
  showBlueprint: true,
  child: Column(...),
)
```

---

## 4. Rollback Procedure

Because nothing was overwritten, rollback is always zero-risk.

**Web:** restore `template_folder="templates"` in `app.py` (Option A) or delete `templates_v2/` and `static/css/bim-v2.css` entirely.

**Mobile:** revert the two lines in `main.dart` to use `AppTheme.light` / `AppTheme.dark` and the original import. `app_theme.dart` is untouched.

**Full wipe:**
```bash
rm -rf C:\Users\Kishan\BIM_CRM\templates_v2
rm     C:\Users\Kishan\BIM_CRM\static\css\bim-v2.css
rm     C:\Users\Kishan\BIM_Mobile\lib\core\theme\app_theme_v2.dart
rm     C:\Users\Kishan\BIM_CRM\README-REDESIGN.md
```

---

## 5. Design System Reference

### Core Tokens (CSS + Flutter)

| Token | CSS | Flutter | Usage |
|---|---|---|---|
| Canvas | `--bg-0: #000000` | `BimColorsV2.bg0` | App background, page root |
| Surface | `--bg-1: #0A0A0A` | `BimColorsV2.bg1` | Cards, nav, drawers |
| Raised | `--bg-2: #121212` | `BimColorsV2.bg2` | Inputs, chips, secondary cards |
| Ink 0 | `--ink-0: #FAFAFA` | `BimColorsV2.ink0` | Primary text, headings |
| Ink 1 | `--ink-1: #D4D4D4` | `BimColorsV2.ink1` | Body text |
| Ink 2 | `--ink-2: #A3A3A3` | `BimColorsV2.ink2` | Secondary text, labels |
| Ink 3 | `--ink-3: #737373` | `BimColorsV2.ink3` | Metadata, eyebrows |
| Gold | `--gold: #E8B84C` | `BimColorsV2.gold` | Primary brand accent |
| Cyan | `--cyan: #22D3EE` | `BimColorsV2.cyan` | Info, scan/data accent |
| Moss | `--moss: #84CC16` | `BimColorsV2.moss` | Success, Won status |
| Ember | `--warn: #F59E0B` | `BimColorsV2.ember` | Warnings, Warm status |
| Rose | `--rose: #F43F5E` | `BimColorsV2.rose` | Hot, danger, alerts |

### Type Stack

- **Display** · Space Grotesk (600) · eyebrows, headlines, KPI values, hero copy
- **Body** · Inter (400–600) · paragraphs, UI labels, nav
- **Mono** · JetBrains Mono (400–500) · metadata chips, code, timestamps, technical labels

All three are loaded from Google Fonts in both the web (via `<link>` tags in `base.html`) and Flutter (via the `google_fonts` package in `app_theme_v2.dart`).

### Radii

| Size | CSS var | Flutter const |
|---|---|---|
| xs | `--r-xs: 2px` | `BimRadiusV2.xs` |
| sm | `--r-sm: 4px` | `BimRadiusV2.sm` |
| md | `--r-md: 8px` | `BimRadiusV2.md` |
| lg | `--r-lg: 12px` | `BimRadiusV2.lg` |
| xl | `--r-xl: 16px` | `BimRadiusV2.xl` |

Heritage-industrial brands read as more credible with tight, almost-square corners. 4–8px is the sweet spot; we avoid pill-soft 12px+ on primary UI.

### Signature Motifs

**Blueprint grid** — 56px × 56px faint gold grid fading to transparent at the edges. Used on the login hero and available in Flutter via `BimCard(showBlueprint: true)`. Communicates precision engineering.

**Gold-pip chip** — 8px colored dot + uppercase tracked mono label. Used for status, priority score, country, template. Scannable at a glance.

**Stage-tinted kanban** — each column carries its own `--stage-color` CSS variable, which cascades into the card left-border accent and the circular stage indicator. Lets the eye track status without reading.

**SVG sparklines** — 96×28 inline SVG lines over a gold-to-transparent gradient fill. Loaded inline (no library bloat). Easy to parameterize with `d="M0,14 L24,12 L48,9 ..."` polylines.

---

## 6. Extending to Remaining Templates

The rest of the template set (projects, invoices, expenses, income, team, team_tasks, reminders, templates, replies, settings, my_settings, change_password, lead_detail, lead_form, lead_to_project, activity_log, chat_widget, import, instructions, prompt_generator, responsibilities) can be migrated using the same patterns:

### Starter recipe

1. Create `templates_v2/<page>.html`.
2. Start with:
   ```jinja
   {% extends "base.html" %}
   {% block title %}<Page> · BIM Infra{% endblock %}
   {% block content %}
   <div class="page-header rise">
     <div>
       <div class="eyebrow">MODULE · Subtitle</div>
       <h1 class="bim-h1">Page Title</h1>
     </div>
     <div class="d-flex gap-2">
       <a href="..." class="btn btn-bim">Primary Action</a>
     </div>
   </div>
   ...
   {% endblock %}
   ```
3. Use these ready-made classes from `bim-v2.css`:
   - `.kpi-card` · KPI tile with label/value/delta
   - `.chip`, `.chip.gold`, `.chip.cyan`, `.chip.success`, `.chip.warn`, `.chip.danger` · status pills
   - `.card.rise`, `.rise-2`, `.rise-3` · surface cards with stagger-in animation
   - `.eyebrow` · section label (mono 11px uppercase tracked)
   - `.bim-h1`, `.bim-h2` · display headings
   - `.btn-bim` · primary gold gradient button
   - `.progress` · thin line progress bar
   - `.mono`, `.text-ink-0/1/2/3/4` · typography helpers

4. Match the Jinja contract of the original template — the new file must read the **same** context variables that the Flask view is already passing. Don't change view code.

### Template-by-template notes

- **projects.html, invoices.html** — use card-grid + status chips, kanban-style if projects have phases.
- **expenses.html, income.html** — use KPI strip at top + sortable table (inherit leads.html table skin).
- **team.html, team_tasks.html** — avatar-initial rows (reuse the pattern from leads.html), chip tags for role/department.
- **reminders.html** — timeline layout: left-border accent in rose for overdue, gold for today, cyan for upcoming.
- **templates.html, replies.html** — code-editor feel: JetBrains Mono body, gold accent for variable placeholders.
- **settings.html, my_settings.html, change_password.html** — narrow single-column form panel (reuse login.html right side).
- **lead_detail.html** — two-column: left detail cards, right activity timeline.
- **chat_widget.html** — floating glass panel, gold-gradient send button.

---

## 7. Design Rationale

**Why pure black?**  BIM Infra operates at the intersection of heritage conservation (centuries-old buildings) and digital twin precision (millimeter LiDAR scans). A pure black canvas signals seriousness, reduces eye fatigue during long point-cloud review sessions, and photographs beautifully on the PPT decks the team shares with ministry clients.

**Why heritage gold (#E8B84C)?**  It references architectural brass, gilded heritage ornamentation, and the golden-section proportions common in classical architecture — without tipping into ostentatious luxury. Desaturated enough to work on black without vibrating.

**Why blueprint grid?**  Every BIM project starts from coordinates. The faint 56px grid on the login hero is a subconscious signal: *this is an engineering product, not a consumer CRM*.

**Why Space Grotesk + Inter + JetBrains Mono?**  Space Grotesk's tight geometric display has the right "architectural drafting" feel for headlines. Inter is the industry default for UI body at this quality tier. JetBrains Mono in chips and metadata reads as data, not decoration — reinforcing the technical competence positioning.

**Why tracked, uppercase mono eyebrows?**  They function as section drawings' title-block labels. Architects and engineers recognize the pattern instantly.

---

## 8. What's Next

**Priority migration queue** (ordered by user-visible impact):

1. `lead_detail.html` — the single most-viewed page in the CRM after dashboard
2. `projects.html` + `lead_to_project.html` — the delivery side of the business
3. `invoices.html` + `invoice_form.html` + `invoice_view.html` — revenue visibility
4. `team.html` + `team_tasks.html` + `reminders.html` — internal ops hub
5. `templates.html` + `replies.html` — email command center
6. Settings suite (`settings.html`, `my_settings.html`, `change_password.html`)
7. Misc (`activity_log.html`, `prompt_generator.html`, `instructions.html`, `responsibilities.html`, `import*.html`)

**Flutter follow-ups:**

- Showcase dashboard screen using `BimKpiCard` grid + `BimCard` activity feed
- Lead detail screen with the two-column layout mirrored from web
- Pipeline kanban — Flutter `PageView` + drag-drop using `flutter_slidable` or `reorderable_grid_view`
- Custom app-bar with the blueprint-grid gold mark (SVG equivalent)

**Marketing site (biminfrasolutions.in):**  Once the portal + app are fully migrated, the public site can be rebuilt using the same `bim-v2.css` tokens as the shared design language. That's the third phase of this program.

---

## 9. Using Your Exact Logo PNG

The pack ships with `static/img/bim-mark.svg` — a faithful SVG recreation of the silver ribbon "b" mark. It renders crisply at every size. Every template uses an `onerror` fallback that automatically tries `bim-mark.png` if you drop one in.

**To swap in the original raster:**

1. Save your ribbon "b" PNG (transparent background, 512×512 or larger recommended) to:
   ```
   C:\Users\Kishan\BIM_CRM\static\img\bim-mark.png
   ```
2. That's it. Every page picks it up automatically — no code changes.

**Optional — if you want PNG only (skip SVG):**
```html
<!-- In base.html, replace the brand-mark img tag with: -->
<img src="{{ url_for('static', filename='img/bim-mark.png') }}" class="brand-mark">
```

**Preview the logo at all sizes:** open `static/img/LOGO-PREVIEW.html` in your browser — it shows the mark at 16, 32, 48, 80, 160, 320 px plus on-light and on-gold backgrounds.

**Logo placements already wired:**
- `base.html` — sidebar top-left (40×40, hover = rotate-4deg scale-1.04)
- `login.html` — hero brand mark (54×54) + giant watermark (640×640 at 5.5% opacity behind headline)
- `dashboard.html` — top-right corner watermark (220×220 at 7% opacity, rotated -8deg)
- `favicon.svg` — browser tab icon (32×32 rounded square with the mark)

**Flutter app:**
The mark is rendered natively via `BimBrandMark` widget (CustomPainter, zero dependencies). Use it anywhere:
```dart
// In app bar
AppBar(title: BimBrandLockup(markSize: 32))

// In drawer header
DrawerHeader(child: BimBrandMark(size: 64))

// In splash screen with gold variant
BimBrandMark.gold(size: 120)

// Monochrome for dense UI
BimBrandMark.mono(size: 20, color: Colors.white70)
```
SVG assets are also copied to `assets/images/bim-mark.svg` in the Flutter project in case you prefer to use them via `flutter_svg` later.

---

## 10. Contact & Questions

This pack is built to be self-service. If you hit any edge case:

- **CSS variable not rendering?**  The legacy aliases in `bim-v2.css` cover old base.html. If a specific legacy var is missing, add it to the `:root` block.
- **Jinja block error?**  `templates_v2/base.html` defines `content`, `head_extra`, and `scripts` blocks. Legacy templates that don't wrap their scripts in `{% block scripts %}` will render fine — just paste raw `<script>` before `{% endblock content %}`.
- **Flutter widget mismatch?**  Most material components auto-adopt the theme. For custom widgets, import `BimColorsV2` / `BimTypographyV2` directly.

---

*Blacksmith v2.0 — crafted for BIM Infra Solutions LLP · Heritage precision at command-center speed.*
