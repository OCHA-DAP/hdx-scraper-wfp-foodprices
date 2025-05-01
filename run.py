import subprocess
from os import environ, getcwd
from os.path import join

pythonpath = environ.get("PYTHONPATH")
newpath = join(getcwd(), "src")
environ["PYTHONPATH"] = f"{pythonpath}:{newpath}"

subprocess.run("python3 -m hdx.scraper.wfp.foodprices.country", shell=True, check=True, env=environ)
subprocess.run("python3 -m hdx.scraper.wfp.foodprices.world", shell=True, check=True, env=environ)
