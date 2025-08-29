'''Application py file for final project. Going to be web scraping, extracting, 
loading to tables, transforming, uploading to a database and then runnin' queries!
'''

def log_progress(inp)
    '''Log timestamped entry of function/return etc
    '''
    timestamp_format = "%Y-%b-%d-%H:%M:%S"
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open("./code_log.txt","a") as f:
        f.write(timestamp+":"+inp+"\n")
    

