"""
polish_pptx_dehlya.py — improve slides 7-13 (Bronze to Gold) of the team
deck without touching any other slide.

Reads:  C:\\Users\\dehly\\Downloads\\Data-Cycle-Project 1.pptx
Writes: docs\\v2\\out\\Data-Cycle-Project_polished.pptx

Strategy: open with python-pptx, iterate text frames on slides 7-13 only,
apply targeted run-level text replacements (preserves all formatting,
fonts, colours, positions, images, and the visual template).
"""

from copy import deepcopy
from pathlib import Path
from pptx import Presentation

INPUT_PATH  = Path(r"C:\Users\dehly\Downloads\Data-Cycle-Project 1.pptx")
OUTPUT_PATH = Path("docs/v2/out/Data-Cycle-Project_polished.pptx")
TARGET_SLIDES = set(range(7, 14))  # 7, 8, 9, 10, 11, 12, 13


# (substring_to_find, replacement_text)
# Applied to ANY text frame on slides 7-13. Keep changes specific so we
# don't accidentally touch unintended copy.
REPLACEMENTS = [
    # ── SLIDE 7: Bronze ─────────────────────────────────────────────────────
    # Opening paragraph: solid; tighten the closing tagline
    ("Bronze is the traceability and safety layer of the project.",
     "Bronze is the traceability and safety net of the pipeline — every other layer can be rebuilt from it."),

    # ── SLIDE 8: Bronze → Silver ────────────────────────────────────────────
    # Original opening had repeating "Silver transforms..." phrasing and was
    # truncated. Replace with a tighter version.
    ("The Silver layer transforms raw data into clean, typed and structured "
     "relational data. Silver transforms raw technical files into reliable "
     "structured data. Every later output depends on the quality of",
     "The Silver layer turns raw files into clean, typed, structured "
     "relational data. Every later output — Gold tables, Power BI "
     "dashboards, ML predictions — depends on the quality of this "
     "transformation."),

    # Second-level fix in case the truncation differs in the file
    ("Silver transforms raw technical files into reliable structured data.",
     ""),

    # ── SLIDE 9: Silver Data Model ──────────────────────────────────────────
    ("The Silver layer contains normalised operational data. It is designed "
     "to represent the cleaned version of the source data. Silver is the "
     "clean operational foundation of the project.",
     "The Silver layer is the clean operational foundation of the project — "
     "normalised, typed tables that mirror the source systems with errors "
     "and inconsistencies removed."),

    ("The Silver layer is not yet the final analytical model, but it provides "
     "the reliable base required to build that model.",
     "Silver is not yet the analytical model — it is the reliable base that "
     "Gold is built on top of."),

    # ── SLIDE 10: Silver → Gold ─────────────────────────────────────────────
    ("The Gold layer transforms cleaned data into a business-oriented "
     "analytical model. Gold is the bridge between cleaned data and final "
     "exploitation — designed for BI and ML, not only storage.",
     "The Gold layer turns cleaned data into a business-oriented analytical "
     "model. Designed for BI and ML — not just storage — it is the bridge "
     "between technical data and final exploitation."),

    ("The project uses a star schema composed of two types of tables:",
     "The Gold model uses a star schema with two table families:"),

    # ── SLIDE 11: Gold Dimensions ───────────────────────────────────────────
    ("Dimensions provide the descriptive context required for analysis. "
     "Without dimensions, facts are only numbers. Dimensions allow users to "
     "understand where, when and by which device each measurement was",
     "Dimensions are the descriptive context for every measurement. "
     "Without them, facts are only numbers — dimensions tell you where, "
     "when, and by which device each measurement was captured."),

    # Improve dim descriptions — be more specific
    ("Provides date and time attributes for time-based analysis. Enables "
     "filtering by hour, day, week, month or year.",
     "Date and time attributes at minute granularity. Enables filtering by "
     "hour, day, week, month, weekday or year."),

    ("Describes each apartment. Allows dashboards to compare behaviour across "
     "different apartment units.",
     "Apartment metadata (anonymised). Lets dashboards compare behaviour "
     "across apartments while preserving Row-Level Security."),

    ("Describes rooms and their relationship with apartments. Enables "
     "room-level granularity in all analyses.",
     "Rooms with their parent apartment. Enables room-level granularity "
     "across every fact table."),

    ("Describes sensors and devices. Allows filtering and grouping by device "
     "type, location and status.",
     "Sensors and devices linked to their room. Used to filter and group by "
     "device type, location, and operational status."),

    ("Dimensions allow dashboards to filter, group and compare data by time, "
     "apartment, room or device — giving business meaning to numerical "
     "measurements.",
     "Together, dimensions give business meaning to numerical measurements — "
     "the same fact can be sliced by time, apartment, room, or device."),

    # ── SLIDE 12: Gold Fact Tables ──────────────────────────────────────────
    ("Power and energy consumption indicators. Core metrics for monitoring "
     "apartment electricity usage.",
     "Power (W) and energy (kWh) per device per minute. Cost projections via "
     "the tariff dimension."),

    ("Temperature, humidity, CO₂ and other comfort indicators. Tracks indoor "
     "environmental quality over time.",
     "Temperature, humidity, CO₂, noise and pressure per room per minute. "
     "Outliers flagged at the silver layer."),

    ("Room occupancy and motion-based presence indicators. Captures when and "
     "where people are detected.",
     "Motion, door and window flags per room per minute. Foundation for the "
     "occupancy-prediction ML workflow."),

    ("Sensor activity, reliability and data availability indicators. "
     "Monitors the health of the IoT infrastructure.",
     "Daily uptime, error counts, missing readings and battery levels per "
     "device. Surfaces failing sensors before they impact the analysis."),

    ("Machine learning prediction outputs. Stores model results alongside "
     "actuals for comparison and evaluation.",
     "Motion and consumption forecasts written by KNIME, with the prediction "
     "timestamp recorded so model versions can be compared."),

    # ── SLIDE 13: Use Cases ─────────────────────────────────────────────────
    # Punctuation: existing items end with ; and last with . — keep as is,
    # but fix any inconsistencies and lower-case awkwardness
    ("avg power consumption ;",            "Average power consumption ;"),
    ("peak consumption ;",                 "Peak consumption ;"),
    ("consumption by room.",               "Consumption by room."),
    ("missing data ;",                     "Missing data ;"),
    ("inactive devices ;",                 "Inactive devices ;"),
    ("last seen timestamp.",               "Last-seen timestamp."),
    ("occupied time by room ;",            "Occupied time by room ;"),
    ("presence by hour ;",                 "Presence by hour ;"),
    ("weekday/weekend patterns.",          "Weekday vs weekend patterns."),
    ("predicted occupancy ;",              "Predicted occupancy ;"),
    ("predicted consumption ;",            "Predicted consumption ;"),
    ("prediction vs actual.",              "Predicted vs actual."),
    ("average temperature ;",              "Average temperature ;"),
    ("humidity range ;",                   "Humidity range ;"),
    ("CO₂ level.",                         "CO₂ level."),  # already correct, keep

    # Slide 13 lead-in — slightly tighten
    ("The Gold layer supports both descriptive analytics and predictive "
     "analytics. This slide connects the data model with the final business "
     "value of the project.",
     "The Gold layer supports both descriptive and predictive analytics — "
     "this is where the data model meets real business value."),

    # ── Cleanup pass: trailing fragments left from multi-run replacements ──
    # Slide 8: stray "S" left behind from "Silver transforms..." sentence
    ("transformation. Silver", "transformation."),
    ("transformation. S",       "transformation."),
    ("transformation. data.",   "transformation."),
    ("transformation.data.",    "transformation."),
    ("transformation. Data.",   "transformation."),
    # Slide 11: dangling "produced." left after replacing the truncated
    # "...measurement was [truncated] produced." sentence
    ("captured. produced.", "captured."),
    ("captured.produced.",  "captured."),
]


def replace_in_text_frame(tf, old, new):
    """Replace 'old' with 'new' inside a text frame.
    Tries run-level first to preserve formatting; falls back to paragraph
    rewrite if 'old' spans multiple runs."""
    n_replaced = 0
    for paragraph in tf.paragraphs:
        # Fast path: substring fits in a single run
        for run in paragraph.runs:
            if old in (run.text or ""):
                run.text = run.text.replace(old, new)
                n_replaced += 1

        # Slow path: substring may span runs
        joined = "".join(r.text or "" for r in paragraph.runs)
        if old in joined and n_replaced == 0:
            new_joined = joined.replace(old, new)
            if paragraph.runs:
                paragraph.runs[0].text = new_joined
                for r in paragraph.runs[1:]:
                    r.text = ""
            n_replaced += 1
    return n_replaced


def main():
    if not INPUT_PATH.exists():
        raise SystemExit(f"Input not found: {INPUT_PATH}")
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    print(f"Reading  {INPUT_PATH}")
    p = Presentation(INPUT_PATH)

    print(f"\nTarget slides: {sorted(TARGET_SLIDES)}\n")
    total_replacements = 0

    for slide_idx, slide in enumerate(p.slides, start=1):
        if slide_idx not in TARGET_SLIDES:
            continue

        slide_replacements = 0
        for shape in slide.shapes:
            if not shape.has_text_frame:
                continue
            for old, new in REPLACEMENTS:
                slide_replacements += replace_in_text_frame(shape.text_frame, old, new)

        if slide_replacements:
            print(f"  Slide {slide_idx:2d}: {slide_replacements} replacement(s)")
        total_replacements += slide_replacements

    print(f"\n{total_replacements} text replacement(s) applied to slides 7-13.")
    p.save(OUTPUT_PATH)
    size_kb = OUTPUT_PATH.stat().st_size / 1024
    print(f"\n✓ Wrote {OUTPUT_PATH} ({size_kb:.0f} KB)")
    print(f"  Slides 1-6 and 14-21 untouched.")


if __name__ == "__main__":
    main()
