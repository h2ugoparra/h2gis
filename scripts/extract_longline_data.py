from pathlib import Path
from loguru import logger
import pandas as pd


from h2gis import settings
from h2gis.processing import Extractor

def main():  
    log_path = settings.LOGS_DIR / f"{Path(__file__).stem}.log"
    logger.add(log_path, level="DEBUG")  
    
    file_path = Path(r"C:\Users\h2ugo\Documents\COSTA\longline\data\processed\multilines.shp")
    
    logger.info(f"Starting extraction process for {file_path}")

    #var_dict = {
    #    'seapodym': None
    #}
    
    extractor = Extractor(file_path, time_col='ls_date', index_col='idlance')
    
    n_workers = mp.cpu_count()
    
    out_file_name = settings.EXTERNAL_DIR / "LL_extracted_new.csv"
    extractor.run(output_path=out_file_name, n_workers=n_workers)
    
    
if __name__ == "__main__":
    import multiprocessing as mp
    mp.freeze_support() 
    main()