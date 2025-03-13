import re
from difflib import SequenceMatcher
from typing import Dict

from hdx.utilities.matching import multiple_replace


def match_source(sources, source):
    words = source.split(" ")
    if len(words) < 2:
        return False
    found = False
    for cursource in sources:
        words = cursource.split(" ")
        if len(words) < 2:
            continue
        seq = SequenceMatcher(None, source, cursource)
        if seq.ratio() > 0.9:
            found = True
    return found


def process_source(sources: Dict, orig_source: str):
    replacements = {"M/o": "Ministry of", "+": "/"}
    orig_source = multiple_replace(orig_source, replacements)
    regex = r"Government.*,(Ministry.*)"
    match = re.search(regex, orig_source)
    if match:
        split_sources = [match.group(1)]
    else:
        replacements = {",": "/", ";": "/"}
        split_sources = multiple_replace(orig_source, replacements).split("/")
    for source in split_sources:
        source = source.strip()
        if not source:
            continue
        if source[-1] == ".":
            source = source[:-1]
        source_lower = source.lower()
        if "mvam" in source_lower and len(source_lower) <= 8:
            source = "WFP mVAM"
        elif "?stica" in source:
            source = source.replace("?stica", "ística")
        source_lower = source.lower()
        if not match_source(sources.keys(), source_lower):
            sources[source_lower] = source
