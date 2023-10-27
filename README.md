# BFI_scripts [WIP]
Welcome to the BFI Scripts Repository! This repository contains a collection of scripts developed by the British Film Institute (BFI) National Archive for various digital preservation and data management tasks.


# Contents

1. [Introduction](#-Introduction)
2. [Scripts overview](#-Scripts-Overview)
3. [Getting started](#-Getting-Started)
4. [Usage](#-Usage)
5. [Contributing](#-Contributing)
6. [License](#-License)
7. [Contact](#-Contact)

# Introduction


# Script Repository Overview


# Getting started

If you would like to download and try some of the scripts in this repository then please follow the steps below. You may need to sign up for a GitHub account and configure git in your workspace.

### Clone the repository
`git clone https://github.com/bfidatadigipres/BFI_scripts.git`

### Change directory to the repository folder
`cd BFI_scripts`

### Create a Python VENV for safe installation of packages
For more information visit the [Python VENV installation page.](https://packaging.python.org/en/latest/guides/installing-using-pip-and-virtual-environments/)
You may need to upgrade your version of pip before proceeding.

`python3 -m pip install --user virtualenv`  
`python3 -m venv ENV`  
`source ENV/bin/activate`  

Once you've activated your ENV you can safely start to install the Python dependencies for this repository.

### Install dependencies
`python3 -m pip install requests`  
`python3 -m pip install tenacity`  
`python3 -m pip install dicttoxml`  
`python3 -m pip install lxml`  
`python3 -m pip install pytz`
`python3 -m pip install `

There are some open source software installations required for using this repository which can be downloaded direct from their websites:  
[FFmpeg open-source video encoder/decoder, muxer and streaming tool.](https://ffmpeg.org)  
[MediaInfo from Media Area. Open-source metadata extractor.](https://mediaarea.net/mediainfo)  
[MediaConch from Media Area. Metadata compliance checker.](https://mediaarea.net/mediaconch)  
[Graphic Magick image manipulation tool with CLI.](http://www.graphicsmagick.org/download.html)   

Linux tools that are used include:  


Additional requirements:  
[Python D3 SDK for SpectraLogic tape library integrations.](https://github.com/SpectraLogic/ds3_python_sdk)  

# Usage


# Contributing


# License
These scripts are available under the MIT licence. They are live code and therefore under continual redevelopment and as a result will contain untested features within the code. If you wish to use these yourself please create a safe environment to use this code separate from preservation critical files. All comments and feedback welcome in our [discussion page!](https://github.com/bfidatadigipres/BFI_scripts/discussions)


# Contact
