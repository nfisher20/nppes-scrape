import requests
import os
from bs4 import BeautifulSoup
import glob
import zipfile
import sys


def download_url(url, save_path, chunk_size=128):
    r = requests.get(url, stream=True)
    with open(save_path, 'wb') as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            fd.write(chunk)

def DownloadAndUnzipNPPESData(savefolder):
    #accesses the NPPES website and pulls all a tags and their corresponding href, which is the csv files location
    headers = {
        'User-Agent': 'Username, domain.com',
        'From': 'job@email.com'
    }

    URL = "https://download.cms.gov/nppes/NPI_Files.html"
    r = requests.get(URL, headers=headers)

    if r.status_code == 200:
        print('Successful')

    soup = BeautifulSoup(r.content, 'html5lib')

    last_links = soup.find(class_='skip')
    last_links.decompose()

    links = []
    for link in soup.find_all('a'):
        links.append(link.get('href'))

    #goes through the links pulled from NPPES and removes any non-zip files as well as Monthly data files (is numeric function)
    for link in reversed(links):
        if '.zip' not in link.lower() or ('Deactivated' not in link and link.split('_')[4].isnumeric() is False):
            links.remove(link)

    #the links come in a ./pathtocsv format that needs to be appended to the abbrURL
    abbrURL = 'https://download.cms.gov/nppes'

    #returns files in the current directory and removes links if they exist, eliminating duplicate downloads
    folders = [dir for r, d, f in os.walk(savefolder) for dir in d]

    for link in reversed(links):
        for f in folders:
            if f in link:
                links.remove(link)

    #exits the python script if all links have been removed from links, meaning no new downloads
    if len(links) == 0:
        sys.exit("No new files to download")

    #downloads all links left in the links list
    for link in links:
        try:
            download_url(str(abbrURL + link[1:]), savefolder + '\\' + link[2:])
        except:
            print(f'Previously downloaded: {link[1:]}')

    #tried to unzip using folder variables but recycled links list instead

    # toUnzip = glob.glob(savefolder + "/*.zip")
    # pathLink = zip(toUnzip, links)

    #unzips all downloaded files and removes old zip directories, link[2:] slices the link and removes the ./ at the beginning
    for link in links:
        try:
            with zipfile.ZipFile(savefolder + '\\' + link[2:], 'r') as zip_ref:
                zip_ref.extractall(savefolder + link[1:link.find('.zip')])
            os.remove(savefolder + '\\' + link[2:])
        except:
            print(f'{link[2:]} already extracted')


    filestoupload = {}

    #more complex than it needs to be, builds a dictionary of files to be uploaded to the blob from the unzipped directories
    for link in links:
        for d, r, f in os.walk(savefolder + '\\' + link[2:].replace('.zip', '')):
            filestoupload[link[2:].replace('.zip', '').replace('_', '')] = []
            for file in f:
                filestoupload[link[2:].replace('.zip', '').replace('_', '')].append(
                    [os.path.join(savefolder + '\\' + link[2:].replace('.zip', ''), file), file])

    return filestoupload
