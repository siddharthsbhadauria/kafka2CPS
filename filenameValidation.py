import re
import os
import logging

logging.basicConfig(level=logging.INFO)

def verify_file_name(file_path):
    try:
        file_name, _ = os.path.splitext(os.path.basename(file_path))
        pattern = r'^\w+-\w+-\w+-\w+-\d{2}-\d{8}-\w+$'
        if re.match(pattern, file_name, re.IGNORECASE):
            logging.info("File name follows the convention.")
            return True
        else:
            logging.error("File name does not follow the convention.")
            return False
    except Exception as e:
        logging.error(f"Error occurred: {e}")
        return False

# Example usage
if __name__ == "__main__":
    file_path = "/path/to/customer-corebanking-np-uni1-01-20240101-Overview.csv.gpg"
    if verify_file_name(file_path):
        print("The file name follows the convention.")
    else:
        print("The file name does not follow the convention.")