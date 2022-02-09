#!/usr/bin/env python
import setuptools

main_ns  = {}
ver_path = setuptools.convert_path("aws_atmo/version.py")
with open(ver_path) as ver_file:
  exec(ver_file.read(), main_ns)

setuptools.setup(
  name                 = "aws_atmo",
  description          = "Package for downloading Atmospheric/Meteorologic data from AWS",
  url                  = "https://github.com/kwodzicki/aws_atmo",
  author               = "Kyle R. Wodzicki",
  author_email         = "krwodzicki@gmail.com",
  version              = main_ns['__version__'],
  packages             = setuptools.find_packages(),
  install_requires     = [ "boto3", "pyyaml"],
  include_package_data = True,
   package_data        = {"" : ["data/*.xml", "data/*.txt", "data/*.json", "data/*.yml"]},
  scripts              = ["bin/aws_gfs_download",
                          "bin/aws_hrrr_download"],
  zip_safe             = False,
)
