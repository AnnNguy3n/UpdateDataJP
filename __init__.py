import os
import sys
from selenium import webdriver

options = webdriver.EdgeOptions()
options.add_experimental_option("excludeSwitches", ["enable-logging"])
# options.add_argument("--headless")

ROOT_PATH = os.path.dirname(os.path.abspath(__file__))

try: sys.path.remove(os.getcwd())
except: ...

if ROOT_PATH not in sys.path:
    sys.path.append(ROOT_PATH)
