### imports
import pandas as pd
import numpy as np
import requests
import xmltodict
from bs4 import BeautifulSoup
from lxml import etree
from time import sleep
from dotenv import load_dotenv
import os


### global variables
# URL for the SEC's EDGAR database for a certain company based on their CIK
# CIK means Central Index Key
load_dotenv()
USER_AGENT = os.getenv('USERAGENT')
EDGAR_URL = "https://www.sec.gov/edgar/browse/?CIK="
RSS_URL = "https://data.sec.gov/rss?cik={thisCik}&count={count}"
DESIRED_RECORD_COUNT = 40

def send_request(url: str) -> requests.Response:
    headers = {
        'User-Agent': USER_AGENT,
        'Accept-Encoding': 'gzip, deflate'
    }
    response = requests.get(url, headers=headers)
    return response

### functions
def main():
    # Get the list of CIKs
    ciks = get_ciks()
    # Get the XML content for each CIK
    for cik in ciks:
        data = get_xml_content(cik)
        records_13F_HR = get_13F_HRs_filling_info(data)
        links = get_13F_HR_links(records_13F_HR)
        save_13F_HR_data(links)

# get_ciks() returns a list of CIKs from the csv file
def get_ciks() -> list[int]:
    # read in the CIKs from the csv file
    ciks = pd.read_csv("ciks.csv")
    ciks_list = [int(cik) for cik in ciks['CIK']]
    return ciks_list

def get_xml_content(cik: int):
    sleep(1)
    rss = send_request(RSS_URL.format(thisCik=cik, count=DESIRED_RECORD_COUNT))
    content = rss.content
    data = xmltodict.parse(content)
    return data

def get_13F_HRs_filling_info(data: dict) -> list[dict]:
    sleep(1)
    records_13F_HR = []
    for entry in data['feed']['entry']:
    # Check if the entry is of type "13F-HR"
        if entry['category']['@term'] == "13F-HR":
            # Store the relevant content in a dictionary
            record = {
                "acceptance_date_time": entry['content-type']['acceptance-date-time'],
                "accession_number": entry['content-type']['accession-number'],
                "filing_date": entry['content-type']['filing-date'],
                "filing_href": entry['content-type']['filing-href'],
                "form_name": entry['content-type']['form-name'],
                "report_date": entry['content-type']['report-date'],
                "size": entry['content-type']['size']
            }
            records_13F_HR.append(record)
    return records_13F_HR

def get_13F_HR_links(records: list[dict]) -> list[str]:
    list_of_13F_HR_links = []

    for record in records:
        url = record['filing_href']
        sleep(1)
        response = send_request(url)
        html_content = response.content
        soup = BeautifulSoup(html_content, 'html.parser')

        # Get both XML links
        xml_links = []
        for link in soup.find_all('a', href=True):
            if link['href'].endswith('.xml'):
                xml_links.append(link['href'])
        if len(xml_links) > 3:
            first_xml_url = "https://www.sec.gov" + xml_links[1]
            second_xml_url = "https://www.sec.gov" + xml_links[3]
        else:
            raise Exception("Error finding XML links in the 13F filing")
        
        response = send_request(first_xml_url)
        xml_data = response.content
        xml_root = etree.fromstring(xml_data)
        
        # Extract the name and date
        namespace = {'ns': 'http://www.sec.gov/edgar/thirteenffiler'}

        # Extract name
        name_element = xml_root.find('.//ns:filingManager/ns:name', namespaces=namespace)
        assert name_element is not None 
        name = name_element.text

        # Extract date
        date_element = xml_root.find('.//ns:periodOfReport', namespaces=namespace)
        assert date_element is not None
        date = date_element.text

        list_of_13F_HR_links.append([name, date, second_xml_url])

    return list_of_13F_HR_links

def save_13F_HR_data(links: list[str]):
    for link in links:
        pandasDB = extract_trading_info(link[2])
        # Save the extracted data to a CSV file
        pandasDB.to_csv("out/{name}_{date}_13F_HR.csv".format(name=link[0], date=link[1]))

def extract_trading_info(url: str) -> pd.DataFrame:
    data = []
    sleep(1)
    response = send_request(url)       
    xml_data = response.content
    xml_root = etree.fromstring(xml_data)
    namespaces = {'ns': 'http://www.sec.gov/edgar/document/thirteenf/informationtable'}

    for info_table in xml_root.findall('.//ns:infoTable', namespaces=namespaces):
        issuer = info_table.find('ns:nameOfIssuer', namespaces).text
        title_of_class = info_table.find('ns:titleOfClass', namespaces).text
        cusip = info_table.find('ns:cusip', namespaces).text
        value = info_table.find('ns:value', namespaces).text
        ssh_prnamt = info_table.find('ns:shrsOrPrnAmt/ns:sshPrnamt', namespaces).text
        ssh_prnamt_type = info_table.find('ns:shrsOrPrnAmt/ns:sshPrnamtType', namespaces).text
        investment_discretion = info_table.find('ns:investmentDiscretion', namespaces).text
        voting_authority_sole = info_table.find('ns:votingAuthority/ns:Sole', namespaces).text
        voting_authority_shared = info_table.find('ns:votingAuthority/ns:Shared', namespaces).text
        voting_authority_none = info_table.find('ns:votingAuthority/ns:None', namespaces).text

        data.append({
            "nameOfIssuer": issuer,
            "titleOfClass": title_of_class,
            "cusip": cusip,
            "value": int(value),
            "sshPrnamt": int(ssh_prnamt),
            "sshPrnamtType": ssh_prnamt_type,
            "investmentDiscretion": investment_discretion,
            "votingAuthoritySole": int(voting_authority_sole),
            "votingAuthorityShared": int(voting_authority_shared),
            "votingAuthorityNone": int(voting_authority_none),
        })

    return pd.DataFrame(data)

if __name__ == "__main__":
    main()