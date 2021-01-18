import pandas as pd


#function that parameterizes the pandas melt function and splits the record column
from azure.core.exceptions import ResourceExistsError


def meltAndRemoveText(dataframe, pivotlist, variable, value):
    df = pd.melt(dataframe,
                 id_vars=['NPI'],
                 value_vars=pivotlist,
                 var_name=variable,
                 value_name=value)
    df[variable] = df[variable].str.split('_').str[1]

    return df

#columns that belonged to different groupings of prescriber information were spread throughout the csv
#this method cuts the original csv into multiple dataframes, unpivots the data, then joins them back into single dataframes

def unpivotNPIData(csvpath,root,filedate,blobclient):

    uploadstatus = True

    df = pd.read_csv(csvpath,dtype = str)

    #prescriberfinal.csv manipulation

    prescriber = df.loc[:,'NPI':'Authorized Official Telephone Number']
    otherprescriber = df.loc[:,'Is Sole Proprietor':'Authorized Official Credential Text']
    certdate = df.loc[:,'Certification Date']
    prescriberfinal = pd.concat([prescriber,otherprescriber,certdate], axis=1)

    prescriberfinalpath = root+'\\'+filedate+'prescriberfinal.csv'
    prescriberfinalblob = filedate+'prescriberfinal.csv'
    prescriberfinal.to_csv(prescriberfinalpath,index=False)

    writeContainer = 'nppesprescriber'
    container_client_prescriber = blobclient.get_container_client(writeContainer)

    try:
        with open(prescriberfinalpath, "rb") as data:
            container_client_prescriber.upload_blob(name=prescriberfinalblob, data=data)
        print(f'Upload or delete succeeded for {prescriberfinalblob}')
    except ResourceExistsError:
        uploadstatus = False
        print(f'Upload or delete failed for {prescriberfinalblob}')

    #prescribertaxonomy manipulation

    taxonomy = df.loc[:,'Healthcare Provider Taxonomy Code_1':'Healthcare Provider Primary Taxonomy Switch_15']
    npi = df.loc[:,'NPI']
    taxgroup = df.loc[:,'Healthcare Provider Taxonomy Group_1':'Healthcare Provider Taxonomy Group_15']
    taxfinal = pd.concat([npi, taxonomy,taxgroup], axis=1)

    columns = list(taxfinal)
    providertaxswitch = [column for column in columns if 'Healthcare Provider Primary Taxonomy Switch' in column] #columns[1:15]
    providertaxcode = [column for column in columns if 'Healthcare Provider Taxonomy Code' in column] #columns[15:30]
    providertaxgroup = [column for column in columns if 'Healthcare Provider Taxonomy Group' in column]
    providerlicensestate = [column for column in columns if 'Provider License Number State Code' in column]
    providerlicensenumber = [column for column in columns if 'Provider License Number' in column and 'Provider License Number State Code' not in column]

    providertaxswdf = meltAndRemoveText(taxfinal,providertaxswitch,'Record','TaxonomySwitch')
    providertaxcodedf = meltAndRemoveText(taxfinal,providertaxcode,'Record','TaxonomyCode')
    providertaxgroupdf = meltAndRemoveText(taxfinal,providertaxgroup,'Record','TaxonomyGroup')
    providerlicensestatedf = meltAndRemoveText(taxfinal,providerlicensestate,'Record','ProviderLicenseState')
    providerlicensenumberdf = meltAndRemoveText(taxfinal,providerlicensenumber,'Record','ProviderLicenseNumber')

    prescriberTaxonomy = providertaxcodedf.merge(providertaxswdf,on=['NPI','Record']).merge(providertaxgroupdf,on=['NPI','Record']).merge(providerlicensestatedf,on=['NPI','Record']).merge(providerlicensenumberdf,on=['NPI','Record'])
    #pd.concat([providertaxcodedf, providertaxswdf], axis=1)
    #prescriberTaxonomy.shape

    taxonomypath = root+'\\'+filedate+'prescriberTaxonomy.csv'
    taxonomyblob = filedate+'prescriberTaxonomy.csv'
    prescriberTaxonomy.to_csv(taxonomypath,index=False)

    writeContainer = 'nppestaxonomy'
    container_client_taxonomy = blobclient.get_container_client(writeContainer)

    try:
        with open(taxonomypath, "rb") as data:
            container_client_taxonomy.upload_blob(name=taxonomyblob, data=data)
        print(f'Upload or delete succeeded for {taxonomyblob}')
    except ResourceExistsError:
        uploadstatus = False
        print(f'Upload or delete failed for {taxonomyblob}')


    #otherproviderinformation manipulation, not loaded to database, not truly needed
    npi = df.loc[:,'NPI']
    otherprovider = df.loc[:,'Other Provider Identifier_1':'Other Provider Identifier Issuer_50']
    otherproviderfinal = pd.concat([npi,otherprovider], axis=1)

    columns = list(otherproviderfinal)
    OtherProviderIdentifier = [column for column in columns if 'Other Provider Identifier_' in column]
    OtherProviderIdentifierCode = [column for column in columns if 'Other Provider Identifier Type Code' in column]
    OtherProviderIdentifierState = [column for column in columns if 'Other Provider Identifier State' in column]
    OtherProviderIdentifierIssuer = [column for column in columns if 'Other Provider Identifier Issuer' in column]

    OtherProviderIdentifier = meltAndRemoveText(otherproviderfinal,OtherProviderIdentifier,'Record','OtherProviderIdentifier')
    OtherProviderIdentifierCode = meltAndRemoveText(otherproviderfinal,OtherProviderIdentifierCode,'Record','OtherProviderIdentifierCode')
    OtherProviderIdentifierState = meltAndRemoveText(otherproviderfinal,OtherProviderIdentifierState,'Record','OtherProviderIdentifierState')
    OtherProviderIdentifierIssuer = meltAndRemoveText(otherproviderfinal,OtherProviderIdentifierIssuer,'Record','OtherProviderIdentifierIssuer')

    prescriberOtherIdentifiers = OtherProviderIdentifier.merge(OtherProviderIdentifierCode,on=['NPI','Record']).merge(OtherProviderIdentifierState,on=['NPI','Record']).merge(OtherProviderIdentifierIssuer,on=['NPI','Record'])
    #pd.concat([providertaxcodedf, providertaxswdf], axis=1)
    #prescriberOtherIdentifiers.shape

    otheridentifierspath = root + '\\' + filedate + 'prescriberOtherIdentifiers.csv'
    otheridentifiersblob = filedate + 'prescriberOtherIdentifiers.csv'
    prescriberOtherIdentifiers.to_csv(otheridentifierspath,index=False)

    writeContainer = 'nppes'
    container_client_write = blobclient.get_container_client(writeContainer)

    try:
        with open(otheridentifierspath, "rb") as data:
            container_client_write.upload_blob(name=otheridentifiersblob, data=data)
        print(f'Upload or delete succeeded for {otheridentifiersblob}')
    except ResourceExistsError:
        uploadstatus = False
        print(f'Upload or delete failed for {otheridentifiersblob}')

    return uploadstatus