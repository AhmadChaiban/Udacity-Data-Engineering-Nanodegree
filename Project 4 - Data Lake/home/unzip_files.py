from zipfile import ZipFile


def unzip_files(zip_file_paths):
    """
        Unzip files, which prepares them to be sent to the s3 bucket
    """
    for i in range(len(zip_file_paths)):
        with ZipFile(zip_file_paths[i][0], 'r') as zf:
           #display the files inside the zip
            zf.printdir()
            #Extracting the files from zip file
            zf.extractall(path = zip_file_paths[i][1])
            
            
if __name__== '__main__':
    zip_file_paths = [
        ['./data/logdata/log-data.zip','./ExtractedData/log_data'], 
        ['./data/songdata/song-data.zip','./ExtractedData/']
    ]
    
    unzip_files(zip_file_paths)