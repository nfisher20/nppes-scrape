import azure_datafactory
import azure_blob
import nppes_scrape
import nppes_extract_csv
import os
import re

#downloads NPPES files and returns a dictionary with files to upload to the blob
filestoupload = nppes_scrape.DownloadAndUnzipNPPESData(os.getcwd())

#initiates the blob client
aytublob = azure_blob.initiateBlobServiceClient()

#initiates the data factory client
aytuadf = azure_datafactory.InitiateDatafactoryClient()

#returns a container client that can be used to upload blobs
nppescontainer = azure_blob.returnContainerClient(aytublob,'nppes')

for key, values in filestoupload.items():
    try:
        #only uploads to blob if it is weekly data, avoids loading monthly data files that are 7 GB +
        if 'Weekly' in key:
            for v in values:
                with open(v[0], "rb") as data:
                    nppescontainer.upload_blob(name=key + '/' + v[1], data=data)
    except:
        print(f'Upload or delete failed for {key}')


for root,directories,files in os.walk(os.getcwd()):
    for file in files:
        #pulls the date from the file name
        date = re.search(r"\d+-\d+",file)

        #searches for the main data file in each weekly data dissemination
        if "npidata_pfile" in file.lower() and "fileheader" not in file.lower():
            path = os.path.join(root, file)

            #unpivots weekly prescriber data, loads to blob, returns boolean variable indicating upload success
            uploaded = nppes_extract_csv.unpivotNPIData(path,root,date.group(),aytublob)

            #check to see if the files uploaded, if they did, trigger pipelines
            if uploaded:
                #runs the NPPESPrescriber and Taxonomy pipelines, which updates the database
                azure_datafactory.RunPipeline(aytuadf,'NPPESPrescriber', 30, 300)
                azure_datafactory.RunPipeline(aytuadf,'NPPESTaxonomy', 30, 500)
