# BFI_scripts
Welcome to the BFI Scripts repository! This repository contains a collection of scripts developed by the BFI National Archive's Data and Digital Preservation department, for various digital preservation and metadata management tasks.


# Contents

1. [Introduction](#-Introduction)
2. [Getting started](#-Getting-Started)
3. [Usage](#-Usage)
4. [License](###-License)
5. [Script overview](#-Script-Overview)


# Introduction

Thanks for visiting. This repository contains the Python scripts used to automate many workflows in the BFI National Archive, typically by interacting with the two core collections systems - the Collections Information Database (CID) and the Digital Preservation Infrastructure (DPI) - using their RESTful Application Programming Interface (API). Some of the scripts represent legacy code that has been converted to Python3, and some are recently created scripts for new projects and workflows. All are currently in use - this repository represents our live workflows. The aim of sharing the code is to offer an opportunity for others to see how we work with open source tools and standards, and in the hope of collaboration that might see our and other's practices develop.

If you aim to try some of this code for your own workflows then please note that the code contains many dependencies that are managed using environmental variables (shown in the code as `os.environ['KEY']`) - including network paths, secret keys and other data. You should provide your own variables as required. Python package requirements are outlined in the dependencies list below. Specific software and hardware dependencies for certain scripts are listed in the Script overview. If you would like to test this code please download and test in a safe environment away from any preservation-critical workflows.


# Getting started

If you would like to download and try some of the scripts in this repository then please follow the steps below. You may need to sign up for a GitHub account and configure git in your workspace first.

### Clone the repository
`git clone https://github.com/bfidatadigipres/BFI_scripts.git`

### Change directory to the repository directory
`cd BFI_scripts`

### Create a Python VENV for safe installation of packages
For more information visit the [Python VENV installation page.](https://packaging.python.org/en/latest/guides/installing-using-pip-and-virtual-environments/)
You may need to upgrade your version of pip before proceeding.

`python3 -m pip install --user virtualenv`
`python3 -m venv ENV`
`source ENV/bin/activate`

Once you've activated your ENV you can safely start to install the Python dependencies for the scripts in this repository.

### Install dependencies
`python3 -m pip install requests`
`python3 -m pip install tenacity`
`python3 -m pip install dicttoxml`
`python3 -m pip install lxml`
`python3 -m pip install pytz`
`python3 -m pip install python-magic`


# Usage

To use this code base for your own tests you will need to install dependencies required for each script. We recommend all Python dependencies are installed into a virtual environment (please see guide above). If installing open-source software that relies on FFmpeg then you should installed FFmpeg first before installing following packages. We recommend using Package Managers for easy install of open-source software, when supported. For Windows you can use https://chocolatey.org/install, and for Unix you can use https://brew.sh/.

This is live code so changes will be made to it frequently and as a result some bugs might appear from time to time! Please use this code in a save environment away from preservation critical workflows and files. Always grateful for feedback or suggestions so do get in touch! Thank you!

### Crontab launch and Flock locks

As we deploy the code on servers running Linux Ubuntu operating system, we use Linux's Flock with our scheduled crontab launches, to ensure that only once instance of a script - or its launcher - is operational at any one time. This prevents accidents with multiple instances of a script working on the same file simultaneously and causing data loss or anomalous outcomes.

### License
These scripts are available under the MIT licence. They are live code and therefore under continual redevelopment and as a result will contain untested features within the code. If you wish to use these yourself please create a safe environment to use this code separate from preservation critical files. All comments and feedback welcome in our [discussion page!](https://github.com/bfidatadigipres/BFI_scripts/discussions)


# Script Overview

The scripts are divided into different sub-directories to group together all scripts that together achieve an objective. A short overview of the directory is provided, followed by a brief description of each script and their relationship, if any. Please visit the scripts themselves for detailed information - found in the comments within the code.

Any additional software/hardware needed for the operation of the scripts within their directory will be quickly noted at the top of each directory, if different to those listed in the [Install dependencies](###-Install-dependencies) section.

## access_copy_creation

This directory contains the code that creates low-bitrate access copies of all video and image preservation master files ingested into the BFI National Archive's Digital Preservation Infrastructure (DPI). There are two Python scripts and two Bash launch scripts. They are generally identical but one set of scripts runs in our off-air TV recording context - which generates approximately 500 video files a day, and has different features to accommodate this.

Dependencies:
[FFmpeg open-source video encoder/decoder, muxer and streaming tool.](https://ffmpeg.org)
[GNU Parallel, parallelisation tool to maximise encoding throughput.](https://www.gnu.org/software/parallel/)
[MediaInfo from Media Area. Open-source metadata extractor.](https://mediaarea.net/mediainfo)
[MediaConch from Media Area. Metadata compliance checker.](https://mediaarea.net/mediaconch)
[Graphic Magick image manipulation tool with CLI.](http://www.graphicsmagick.org/download.html)

### mp4_transcode_launch_script.sh / mp4_transcode_launch_script_stora.sh

These scripts are launched frequently from crontab but the script only launches when the previous run has completed. The shell launch script targets a specific transcode path which is passed as an argument from the crontab launch, along with the amount of parallel jobs wanted for that transcode path. The script then searching in the supplied path for any files, adds them to a list and then using GNU Parallel launches the following Python script against each file path and in batches of parallel jobs using the job number received. This script stays operational until all items in the list have been processed before exiting. The received path name is used to inform th ename of the file list that stores the found file paths.

### mp4_transcode_make_jpeg.py / mp4_transcode_make_jpeg_2.py

For video source files, these scripts create one H.264 MP4 video rendition for viewing in web applications (with close attention to display aspect ratio), and two JPG image renditions - one for thumbnail display in search results, and one larger image for poster display in video playback window. For image sources, they create the thumbnail and poster JPGs only.


## black_pearl

This directory contains scripts that directly interact with Black Pearl, a RESTful API gateway to the Spectra Logic data tape libraries used for long-term preservation storage in the BFI National Archive's Digital Preservation Infrastructure. To communicate they use the Spectra Logic Python SDK, a wrapper for the Spectra Logic DS3 Application Programming Interface (API) - link below. These scripts handle the writing of files to tape, the retrieval of information from the Black Pearl's database, and download of files from data tape. They manage deletion of files in the local network after bit-perfect storage to data tape - confirmed with MD5 checksum comparison. The associated download request scripts can be found in the [dpi_downloader and dpi_downloader_elastic_search](##-dpi_downloader-and-dpi_downloader_elastic_search) directories.

Dependencies:
[Python DS3 SDK for SpectraLogic Black Pearl data tape library interactions.](https://github.com/SpectraLogic/ds3_python_sdk)


## document_en_15907

This directory contains scripts to automate the creation of records in the BFI National Archive's Collections Information Database (CID) via calls to its RESTful API. Records are created in compliance with the [EN 15907](https://filmstandards.org/fsc/index.php/EN_15907) standard (a metadata set for the comprehensive description of moving image works and their manifestations across their lifecycle). JSON metadata is fetched from the REST API of our metadata provider, for the Netflix programming that is being acquired into the collection. That JSON metadata is then used to generate a Work-Manifestation-Item hierarchy with available descriptive metadata, including Series Work parent, and cast / crew where available.


## dpi_downloader and dpi_downloader_elastic_search

This directory contains the scripts to manage file downloads from the BFI National Archive's Digital Preservation Infrastructure (DPI). The solution utilise a front-end / back-end structure. The front-end is a Python Flask web application to allow users to submit requests for file downloads and - optionally - transcodes to selected formats. There are two versions of this front-end app - one writes the request details to a SQLite database, and one writes the request details to an Elasticsearch index. The back-end is a set of Python scripts that restore the requested files from DPI using the RESTful API of the Spectra Logic Black Pearl gateway, via a Python SDK; and optionally transcode to either H.264 MP4 or ProRes MOV, as requested.


## splitting_scripts

This directory contains scripts for processing the FFV1 Matroska files created in high volume videotape digitisation workflows in the BFI National Archive. They automate the splitting of full-tape capture files into programme sections, based on timings metadata that was entered into the Archive's Collections Information Database (CID) by the videotape capture technicians. The scripts model a videotape carrier by finding all Item records associated with the carrier record. They validate the timings metadata aginst a set of rules (for example, a programme end time cannot be earlier than its start time), then use FFMPEG stream copy to create a file for each programme section, with handles at start and end points to ensure that the full videotape content is preserved in the splitting. FrameMD5 is used to ensure lossless stream copy. Finally an FFV1 Matroska Item record is created in the Collections Information Database (CID), and associated with the record for the videotape source Item, in a source <-> derived relationship.
