# BFI_scripts [WIP]
Welcome to the BFI Scripts Repository! This repository contains a collection of scripts developed by the BFI National Archive's Data and Digital Preservation department for various digital preservation and data management tasks.


# Contents

1. [Introduction](#-Introduction)
2. [Getting started](#-Getting-Started)
3. [Usage](#-Usage)
4. [License](###-License)
5. [Script overview](#-Script-Overview)


# Introduction

Thanks for visiting. This repository contains the BFI python scripting used to automate many workflows in the BFI National Archive. Some is legacy code converted to Python3, other is recently built code for new project development. All are currently in use and this repository represents our live workflows. The aim of sharing the code is to offer an opportunity for others to see how we work with open source tools and standards, and with the hope of collaboration that might see our and other's practices develop.

If you're keen to try some of this code for your own workflows then please remember that the code is not agnostic. There are many dependencies on environmental variables (shown in the code as `os.environ['KEY']`) which link to paths and other data. Please see the dependencies below for an understanding of python package requirements, specific software and hardware dependencies for certain scripts are listed in the Script overview. If you would like to test this code please download and test in a safe environment away from any preservation-critical workflows.


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

To follow.

### Crontab launch and Flock locks

As we run our code from Linux Ubuntu operating systems we use Linux's Flock with our repeated crontab launches to ensure that only once instance of a script, or launch script is operational at any one time. This prevents accidents with multiple versions of code working on the same file simultaneously and causing proxy or preservation copy damage, if for example transcoding scripts were impacted.

### License
These scripts are available under the MIT licence. They are live code and therefore under continual redevelopment and as a result will contain untested features within the code. If you wish to use these yourself please create a safe environment to use this code separate from preservation critical files. All comments and feedback welcome in our [discussion page!](https://github.com/bfidatadigipres/BFI_scripts/discussions)


# Script Overview

The scripts are broken into different sub-directories which links the contents, sometimes directly and sometimes generally. A short overview of the directory is provided followed by a brief description of each script and their relationship, if any. Please visit the scripts themselves for more information found in the comments within the code.  

Any additional software/hardware needed for the operation of the scripts within their directory will be quickly noted at the top of each directory, if different to those listed in the [Install dependencies](###-Install-dependencies) section.  

## access_copy_creation

This directory contains the code that creates proxy access copies of all video and image items ingested into the BFI National Archive Digital Preservation Infrastructure (DPI). There are two Python scripts and two Bash launch scripts. They are generally identical but one set of scripts runs against our off-air TV dataset which generates approximately 500 video files a day and so has a couple of different features to accommodate this.

Dependencies:  
[FFmpeg open-source video encoder/decoder, muxer and streaming tool.](https://ffmpeg.org)  
[GNU Parallel, parallelisation tool to maximise encoding throughput.](https://www.gnu.org/software/parallel/)
[MediaInfo from Media Area. Open-source metadata extractor.](https://mediaarea.net/mediainfo)  
[MediaConch from Media Area. Metadata compliance checker.](https://mediaarea.net/mediaconch)  
[Graphic Magick image manipulation tool with CLI.](http://www.graphicsmagick.org/download.html)   

### mp4_transcode_launch_script.sh / mp4_transcode_launch_script_stora.sh

These scripts are launched frequently from crontab but the script only launches when the previous run has completed. The shell launch script targets a specific transcode path which is passed as an argument from the crontab launch, along with the amount of parallel jobs wanted for that transcode path. The script then searching in the supplied path for any files, adds them to a list and then using GNU Parallel launches the following Python script against each file path and in batches of parallel jobs using the job number received. This script stays operational until all items in the list have been processed before exiting. The received path name is used to inform th ename of the file list that stores the found file paths.

### mp4_transcode_make_jpeg.py / mp4_transcode_make_jpeg_2.py

To follow.


## black_pearl

This directory contains script that directly interact with the Black Pearl magnetic tape libraries used for long-term preservation storage at the BFI National Archive. To communicate they use the Spectra Logic DS3 SDK Application Programming Interface (API), link below. These scripts handle the writing to tape, retrieval of information from tape, download and careful deletion of assets from tape. Our bulk downloading request scripts can be found in the [dpi_downloader and dpi_downloader_elastic_search](##-dpi_downloader-and-dpi_downloader_elastic_search) directories.

Dependencies:  
[Python DS3 SDK for SpectraLogic tape library integrations.](https://github.com/SpectraLogic/ds3_python_sdk)  


## document_en_15907

These scripts automate the creation of records in the BFI National Archive's Collections Information Database (CID) via calls to its RESTful API. Records are created in compliance with the [EN 15907](https://filmstandards.org/fsc/index.php/EN_15907) standard (a metadata set for the comprehensive description of moving image works and their manifestations across their lifecycle). JSON metadata is fetched from PA Media's REST API for the Netflix programming being acquired into the collection, and that is used to generate a Work-Manifestation-Item hierarchy with available descriptive metadata, including Series Work parent, and cast and crew where available.


## dpi_downloader and dpi_downloader_elastic_search

These scripts utilise a front-end and back-end structure. The front-end is a Python Flask web application to allow users to submit requests for file downloads from the BFI National Archive's Digital Preservation Infrastructure (DPI). There are two versions of this app - one writes the request details to a SQLite database, and the other writes the request details to an elasticsearch index. The back-end is a set of Python scripts that restore the requested files from DPI using the RESTful API of the Spectra Logic Black Pearl gateway, via the Python SDK; and optionally transcode to either H.264 MP4 or ProRes MOV, as requested.


## splitting_scripts

These scripts automate the processing of FFV1 Matroska files created in high volume videotape digitisation workflows in the BFI National Archive - splitting full-tape capture files into programme sections based on timings metadata that was entered into the Archive's Collections Information Database (CID) by the videotape capture technicians. The scripts model a videotape carrier by finding all Item records associated with the carrier record. They validate the timings metadata aginst a set of rules (for example, a programme end time cannot be before its start time), then use FFMPEG stream copy to create a file for each programme section, with handles at start and end points to ensure that the full videotape content is preserved in the splitting. FrameMD5 is used to ensure lossless stream copy. Finally an FFV1 Matroska Item record is created in the Collections Information Database (CID), and associated with the record for the videotape source Item, in a source <-> derived relationship.
