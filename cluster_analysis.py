import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import os
import xlrd
from datetime import datetime
import warnings
import time
import getpass

# Suppress the specific UserWarning from openpyxl and other general configurations
warnings.filterwarnings(
    "ignore", category=UserWarning, module="openpyxl.styles.stylesheet"
)
pd.set_option("display.max_columns", 50)
pd.set_option("display.max_rows", 100)
pd.options.mode.chained_assignment = None
write_to_log = None
time_now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")


def write_log_file(version_path, message):
    # Function to write a log file.
    logpath = "run.log"
    with open(logpath, "a", encoding="UTF8") as log_file:
        log_file.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}: {message}\n")
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}: {message}")


working_dir = os.path.join(
    r"C:\Users",
    getpass.getuser(),
    r"Sibelco\Pricing PRO-PRT - General\10-Data 2021\Uploads",
)
# working_dir = os.path.join(r'C:\Users\BELDAV00\Sibelco\Pricing PRO-PRT - General\10-Data 2021\Uploads') --> working_dir is the destination folder of the output file


# MAIN BRANCH, all operations are done within this function, in order. The rest are helper functions
def main(settings):
    start_time = datetime.now()
    # we are passing those arguments to this function that can be used at any point in the script:
    # sales -->pricing file split exw/del
    # mdm  -->The new MDM extract from mendix
    # lp  -->active list prices extraction from PRO
    # so  -->Relationship between tagetik legal entity and sales organization
    # zcpr  -->Product price conditions
    # stdcosts --> Extraction from financial powerBI with fixed, variable and depreciation cost
    # sapcosts --> Extraction from SAP with total stamdard cost for all items

    # those 3 lines are to print comments in the log
    global write_to_log
    version_path = working_dir
    write_to_log = lambda msg: write_log_file(version_path, msg)
    write_to_log("Script launched successfully")

    # read sales data
    write_to_log("Reading Sales data")
    sales = read_sales(settings.sales)

    # Get SAPLocation to take SAP delivery wharehouse codes and also get legal entities?
    write_to_log("reading mdm")
    saplocations, saplegalentities = read_mdm(settings.mdm)

    # extract MDM warehouse code from sales
    # merge SAP DWH code to sales dataframe
    write_to_log("Merging DWH sap code to sales data")
    sales["MDM DWH"] = sales["Location Of Distribution"].str[:5].astype(str)
    saplocations["LocationCode"] = saplocations["LocationCode"].astype(str)
    original = len(sales)
    sales = pd.merge(
        sales, saplocations, left_on="MDM DWH", right_on="LocationCode", how="left"
    )
    result = len(sales)
    if original == result:
        pass
    else:
        print(
            "DUPLICATE rows were created when merging with MDM locations. Check that 'LocationCode' column in mdm file contains unique values"
        )

    # read List prices
    write_to_log("Reading List Prices")
    lp = read_list_prices(settings.lp)
    write_to_log("Merging with list prices")
    # Create the Key to merge with list prices
    sales["item-key"] = sales["Item"].str.split(" ", n=1).str[0]
    sales["item-key"] = sales["item-key"].astype(str)
    sales["item-dwh-key"] = sales["item-key"] + "|" + sales["SAPCode"]
    # merge with list prices
    original = len(sales)
    sales = pd.merge(
        sales, lp, left_on="item-dwh-key", right_on="LP-item-dwh-key", how="left"
    )
    result = len(sales)
    if original == result:
        pass
    else:
        print(
            "DUPLICATE rows were created when merging with List-prices. Check that the keys item-DWH in LP file are unique"
        )

    # read Sales Org PowerBI extraction to get SO SAP codes
    write_to_log("Reading Sales Org SAP")
    sales_org = read_sales_org(settings.so)

    # Create keys to merge SAP sales org code
    write_to_log("Merging Sales Org SAP Code")
    sales["tagetik-key"] = sales["Tagetik Legal Entity"].str.split(" ", n=1).str[0]
    original = len(sales)
    sales = pd.merge(
        sales, sales_org, left_on="tagetik-key", right_on="legalentitycode", how="left"
    )
    result = len(sales)
    if original == result:
        pass
    else:
        print(
            "DUPLICATE rows were created when merging with Sales-Org file. Check that the column 'legalentitycode' in sales org file contains unique values"
        )

    # remove empty customers and financial customers
    write_to_log("Removing non valid customers")
    condition1 = sales["Country Hierarchy - Customer"] != "-"
    condition2 = (
        ~sales["Country Hierarchy - Customer"].astype(str).str.startswith("SLM_")
    )
    sales = sales[condition1 & condition2]

    # create ZCPR key to merge with pricing conditions
    write_to_log("reading ZCPR conditions")
    conditions = read_zcpr(settings.zcpr)
    write_to_log("Merging ZCPR conditions")
    sales["customer-key"] = (
        sales["Country Hierarchy - Customer"].str.split(" ", n=1).str[0]
    )
    sales["zcpr-key"] = (
        sales["salesorganization"]
        + "|"
        + sales["customer-key"]
        + "|"
        + sales["item-dwh-key"]
    )
    original = len(sales)
    sales = pd.merge(
        sales, conditions, left_on="zcpr-key", right_on="conditions-key", how="left"
    )
    result = len(sales)
    if original == result:
        pass
    else:
        print(
            "DUPLICATE rows were created when merging with ZCPR. Check that the keys sales-org,sold-to,item,DWH in zcpr file are unique"
        )
    # read Financial report for standard costs
    write_to_log("Reading Standard Group Costs")
    stdcosts = read_stdcosts(settings.stdcosts)
    write_to_log("Merging Standard Group Costs")
    original = len(sales)
    sales = pd.merge(
        sales, stdcosts, left_on="item-dwh-key", right_on="stdcosts-key", how="left"
    )
    result = len(sales)
    if original == result:
        pass
    else:
        print(
            "DUPLICATE rows were created when merging with FIN18 costs. Check that the keys item-DWH in fin18 file are unique"
        )
    # read and merge costs from SAP(which should be the same as PBI extraction)
    write_to_log("Reading SAP Costs")
    sapcosts = read_sapcosts(settings.sapcosts)
    write_to_log("Merging SAP Costs")
    original = len(sales)
    sales = pd.merge(
        sales, sapcosts, left_on="item-dwh-key", right_on="sapcosts-key", how="left"
    )
    result = len(sales)
    if original == result:
        pass
    else:
        print(
            "DUPLICATE rows were created when merging with SAP-costs. Check that the keys item-DWH in SAPCosts file are unique"
        )
    # add columns necessary to the analysis
    write_to_log("Enritching the dataframe with KPIs")
    sales["GM_Eur"] = sales["Revenue EXW Pres Curr"] - (
        sales["COGS(depr) Total / Mt"] * sales["Volume Ton CY YTD"]
    )
    sales["CM_Eur"] = sales["Revenue EXW Pres Curr"] - (
        sales["Variable Cost / Mt"] * sales["Volume Ton CY YTD"]
    )
    sales["Deviation_LP_Eur"] = (
        sales["List Price EUR"] - sales["Customer Price EUR/TO"]
    ) * sales["Volume Ton CY YTD"]

    sales["Revenues_with_LP"] = sales["List Price EUR"] * sales["Volume Ton CY YTD"]


    write_to_log("Finalizing File and saving output, this might take a while")
    sales = rename_columns_and_adjustments(sales)
    finalize_and_save(sales)
    endtime = datetime.now()
    total_time = endtime - start_time
    print(f"Total elapsed time: {total_time}")
    write_to_log("Script finished")


def read_sales(file_sales):
    sales_data = pd.read_excel(file_sales, sheet_name="Values vs YTD")
    columns_to_remove = [
        "FCA List Price",
        "List Price currency",
        "Has List Price",
        "Location of Distribution + Item Number",
        "Incoterm Change",
        "Above 50K EUR Customer_Item",
        "Bridge",
        "Bridge EXW",
        "Commercial Hierarchy - Organization Level 6",
        "Commercial Hierarchy - Organization Level 7",
        "Comments",
        "Price Key_Greater 100K EUR",
        "Price Key_excl_Incoterm\ntransactional currency // Customer No. // Tagetik Plant Geography // Item // Incoterm // Ship to",
    ]
    sales_data = sales_data.drop(columns=columns_to_remove)
    # excel produces some inf values, replace those with large numbers
    sales_data["EXW Last Price Pres LY"] = sales_data["EXW Last Price Pres LY"].replace(
        [np.inf, -np.inf], 999999999
    )
    sales_data["Transport Last Price Pres LY"] = sales_data[
        "Transport Last Price Pres LY"
    ].replace([np.inf, -np.inf], 999999999)
    # Exclude joint ventures from the analysis
    sales_data = sales_data[sales_data["JV"] != "YES"]
    return sales_data


def read_mdm(file_mdm):
    saplocations = pd.read_excel(file_mdm, sheet_name="SAPLocations")
    columns_to_keep = [
        "SAPCode",
        "LocationCode",
        "Status",
    ]
    saplocations_clean = saplocations[columns_to_keep]
    legalentities = pd.read_excel(file_mdm, sheet_name="SAPLegalEntities")
    return saplocations_clean, legalentities


def read_list_prices(lp):
    list_prices = pd.read_excel(lp, skipfooter=1)
    columns_to_remove = [
        "Origin Plant",
        "ItemName",
        "Product",
        "Delivery WHS",
    ]
    list_prices = list_prices.drop(columns=columns_to_remove)
    list_prices["ItemNumber"] = list_prices["ItemNumber"].astype("Int64")
    list_prices["LP-item-dwh-key"] = (
        list_prices["ItemNumber"].astype(str) + "|" + list_prices["Del.WHS CODE"]
    )
    return list_prices


def read_sales_org(so):
    sales_org = pd.read_excel(so, skipfooter=1)
    sales_org = sales_org.dropna(subset=["legalentitycode"])
    sales_org = sales_org[sales_org["salesorganization"] != "IT02"]
    sales_org["legalentitycode"] = sales_org["legalentitycode"].astype("Int64")
    sales_org["legalentitycode"] = sales_org["legalentitycode"].astype(str)
    columns_to_remove = ["Legal Entity Code Name", "CONDITIONTYPE", "_RecordCount"]
    sales_org = sales_org.drop(columns=columns_to_remove)
    return sales_org


def read_zcpr(zcpr):
    conditions = pd.read_excel(zcpr, skipfooter=1)
    # strip the codes at the beginning to prepare key
    conditions["Sold-To"] = conditions["Sold-To"].str.split(" ", n=1).str[0]
    conditions["Item"] = conditions["Item"].str.split(" ", n=1).str[0]
    columns_to_remove = [
        "Sold-To Country",
        "Delivery Warehouse Name",
        "Product List Price",
        "List Price Currency",
        "List Price EUR/TO",
        "List Price Valid From",
        "List Price Valid To",
        "List Price Status",
        "Legal Entity",
        "Customer Sales Manager",
        "Price Validity",
        "Created On",
        "Created By",
        "Last Modified On",
        "Last Modified By",
    ]
    conditions = conditions.drop(columns=columns_to_remove)
    conditions["conditions-key"] = (
        conditions["Sales Org"]
        + "|"
        + conditions["Sold-To"]
        + "|"
        + conditions["Item"]
        + "|"
        + conditions["Delivery Warehouse"]
    )
    # remove all duplicates based on ZCPR key to clean data and keep all the relevant customer prices
    conditions = conditions.drop_duplicates(subset=["conditions-key"])
    return conditions


def read_stdcosts(costs):
    stdcosts = pd.read_excel(costs, skipfooter=1)
    columns_to_remove = [
        "Profit Center",
    ]
    stdcosts = stdcosts.drop(columns=columns_to_remove)
    stdcosts["Item Number Name"] = (
        stdcosts["Item Number Name"].str.split(" ", n=1).str[0]
    )
    stdcosts["stdcosts-key"] = (
        stdcosts["Item Number Name"] + "|" + stdcosts["Plant Code"]
    )
    return stdcosts


def read_sapcosts(costs):
    # adjusting the export which comes with empty columns and rows at the beginning
    sapcosts = pd.read_excel(costs, header=4, engine="openpyxl")
    sapcosts.columns = sapcosts.columns.str.strip()
    sapcosts["Price"] = (
        sapcosts["Price"]
        .astype(str)
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
        .str.extract(r"(\d+\.?\d*)")[0]
        .astype(float)
    )
    # Clean 'BUn' and 'Crcy' columns
    sapcosts["BUn"] = (
        sapcosts["BUn"].astype(str).str.strip().replace("...", "", regex=False)
    )
    sapcosts["Crcy"] = (
        sapcosts["Crcy"].astype(str).str.strip().replace("...", "", regex=False)
    )
    columns_to_keep = ["Material", "Plnt", "BUn", "Price", "Crcy"]
    sapcosts = sapcosts[columns_to_keep]
    sapcosts = sapcosts[sapcosts["Material"] != "0"]
    sapcosts["Material"] = sapcosts["Material"].astype("Int64")
    sapcosts["Material"] = sapcosts["Material"].astype(str)
    sapcosts["Plnt"] = sapcosts["Plnt"].str.strip()
    # excluding items we don't need
    sapcosts = sapcosts[
        ~(
            sapcosts["Material"].str.startswith("1")
            | sapcosts["Material"].str.startswith("8")
            | sapcosts["Material"].str.startswith("5")
            | sapcosts["Material"].str.startswith("2")
            | sapcosts["Material"].str.startswith("3")
        )
    ]
    sapcosts["sapcosts-key"] = sapcosts["Material"] + "|" + sapcosts["Plnt"]
    return sapcosts


def finalize_and_save(df1):
    time_now = datetime.now().strftime("%Y-%m-%d_%H-%M")
    savename = time_now + "_cluster_analysis.xlsx"
    with pd.ExcelWriter(savename) as writer:
        df1.to_excel(writer, sheet_name="Database", index=False)


def rename_columns_and_adjustments(df):
    # rename manually everything for clarity
    list_price_columns = [
        "ItemNumber",
        "Packaging",
        "Del.WHS CODE",
        "List Price LOC CURR",
        "Currency Code",
        "List Price EUR",
        "PL.ValidFrom",
        "PL.ValidTo",
    ]
    # This maps 'old_name' : 'new_name'
    rename_lp = {col: "LP_" + col for col in list_price_columns}
    df = df.rename(columns=rename_lp)

    price_report_columns = [
        "Region Of Origin",
        "Subregion Of Origin",
        "TOP KAM",
        "KAM",
        "Tagetik Plant Geography 2021 Hierarchy - Region",
        "Tagetik Plant Geography 2021 Hierarchy - Subregion",
        "Tagetik Plant Geography 2021 Hierarchy - Country",
        "Tagetik Plant",
        "Plant Of Origin",
        "Location Of Distribution",
        "Cluster Of Origin",
        "BL Hierarchy - Sibelco Business Line Name",
        "BL Hierarchy - Sibelco Sub Business Line Name",
        "BL Hierarchy - Sibelco Business Market Name",
        "BL Hierarchy - SIC Code Description",
        "Tagetik Legal Entity",
        "Country Hierarchy - Continent",
        "Country Hierarchy - Country",
        "Country Hierarchy - Customer",
        "Key Account Name",
        "Commercial Hierarchy - Organization Level 1",
        "Commercial Hierarchy - Organization Level 2",
        "Commercial Hierarchy - Organization Level 3",
        "Commercial Hierarchy - Organization Level 4",
        "Commercial Hierarchy - Organization Level 5",
        "Sales Responsible Email",
        "SPC Hierarchy - SPC Group Code Description",
        "SPC Hierarchy - SPC Category Code Description",
        "SPC Hierarchy - SPC Code Description",
        "SPC Hierarchy - Cluster Code Description",
        "Item_x",
        "Incoterm",
        "Tran Curr Code",
        "Customer Segment Code",
        "Shipped To City Name",
        "Last Price Pres LY",
        "ASP Pres CY",
        "ASP Tran CY",
        "Last Price Tran LY",
        "Volume Ton CY YTD",
        "Volume Ton LY FY",
        "Volume Ton LY YTD",
        "Revenue Pres Curr CY YTD",
        "Revenue Pres Curr LY FY",
        "Revenue Pres Curr LY YTD",
        "Revenue Tran Curr CY YTD",
        "Revenue Tran Curr LY FY",
        "Revenue Tran Curr LY YTD",
        "EXW Last Price Pres LY",
        "Transport Last Price Pres LY",
        "Revenue EXW Pres Curr",
        "Transportation Cost (Third party) Pres Curr",
        "EXW Revenue LY FY",
        "Transportation Cost LY FY",
        "Revenue EXW Pres Curr LY",
        "Transportation Cost (Third party) Pres Curr LY",
        "Revenue Pres Curr LY YTD\n@Last Price",
        "Revenue Pres Curr CY YTD\n@Last Price",
        "EXW Revenue Pres Curr LY YTD\n@Last Price",
        "EXW Revenue Pres Curr CY YTD\n@Last Price",
        "EXW Last Price Tran LY",
        "Transport Last Price Tran LY",
        "Revenue EXW Tran Curr",
        "Transportation Cost (Third party) Tran Curr",
        "EXW Revenue Tran LY FY",
        "Transportation Cost Tran LY FY",
        "Revenue EXW Tran Curr LY",
        "Transportation Cost (Third party) Tran Curr LY",
        "EXW ASP Pres CY_v3",
        "EXW ASP Tran CY_v3",
        "Transport ASP Pres CY_v3",
        "Transport ASP Tran CY_v3",
        "FX CY",
        "FX LY",
        "Price Effect %_CALCULATION",
        "Price impact LY YTD",
        "Volume impact LY",
        "FX impact LY YTD",
        "Price impact LY YTD (w_v1)",
        "EXW Price impact LY YTD (w_v1)",
        "Transport Price impact LY YTD (w_v1)",
        "Sold in both periods",
        "Price Impact EXW LY YTD",
        "Volume impact EXW LY",
        "FX impact EXW LY",
        "JV",
        "M&A",
        "GR",
        "Type of Mineral",
        "Price Increase w_v1",
        "Price Increase",
        "Diff",
    ]
    rename_pr = {col: "PR_" + col for col in price_report_columns}
    df = df.rename(columns=rename_pr)

    mdm_columns = ["SAPCode", "LocationCode", "Status"]
    rename_mdm = {col: "MDM_" + col for col in mdm_columns}
    df = df.rename(columns=rename_mdm)

    sales_org_columns = ["legalentitycode", "salesorganization"]
    rename_so = {col: "SALESORG_" + col for col in sales_org_columns}
    df = df.rename(columns=rename_so)

    zcpr_columns = [
        "Sold-To",
        "Sold-To Segment",
        "Sold-To Status",
        "Delivery Warehouse",
        "Item_y",
        "Customer Price",
        "Currency",
        "UoM",
        "Customer Price EUR/TO",
        "Valid From",
        "Valid To",
        "Discount Product List Price (%)",
        "Sales Org",
        "Condition Type",
        "Has Quantity Scaling",
    ]
    rename_zcpr = {col: "ZCPR_" + col for col in zcpr_columns}
    df = df.rename(columns=rename_zcpr)

    stdcosts_columns = [
        "Item Number Name",
        "Variable Cost / Mt",
        "Fixed Cost / Mt",
        "Distribution Cost / Mt",
        "Other / Mt",
        "COGS Total / Mt",
        "Depreciation / Mt",
        "COGS(depr) Total / Mt",
        "Plant Code",
        "ValidFromDate",
        "ValidToDate",
    ]
    rename_stdc = {col: "STDCost_" + col for col in stdcosts_columns}
    df = df.rename(columns=rename_stdc)

    sapcosts_columns = ["Material", "Plnt", "BUn", "Price", "Crcy"]
    rename_sapc = {col: "SAPCost_" + col for col in sapcosts_columns}
    df = df.rename(columns=rename_sapc)

    calculated_columns = [
        "P20_revenues_Segment",
        "Distance_Revenues_P20_LP",
        "P75_Revenues_CY_EXW_Pres_Segment",
        "Distance_Revenues_P75",
        "P75_Contribution_Margin_per_Segment",
        "Distance_CM_P75",
        "Customer_flag_P20Revenues_Deviation",
        "Customer_flag_P75_Revenues",
        "P50_revenues_Segment",
        "P50_CM_segment",
        "P50_PriceInc_segment",
        "Qualification_Flag_CM_Rev",
        "MDM DWH",
        "item-key",
        "item-dwh-key",
        "tagetik-key",
        "customer-key",
        "zcpr-key",
        "GM_Eur",
        "CM_Eur",
        "Deviation_LP_Eur",
        "Revenues_with_LP",
        "conditions-key",
        "stdcosts-key",
        "sapcosts-key",
        "LP-item-dwh-key",
        "Distance_CurrRev_P20Revevues",
        "Distance_P20CM_CustomerCM",
        "Customer_flag_P20CM_Deviation",
        "P20_CM_segment",
        "Qualification_flag_CM_PriceInc",
        "Weighted_CM_perc",
        "P50_CMperc_segment",
        "Qualification_Flag_CMperc_Rev",
        "Qualification_flag_CMperc_PriceInc",
    ]
    rename_calculated = {col: "CALC_" + col for col in calculated_columns}
    df = df.rename(columns=rename_calculated)
    return df


if __name__ == "__main__":

    import argparse

    parser = argparse.ArgumentParser(description=__doc__, epilog="2022, CROPLAND")
    parser.add_argument(
        "--sales",
        required=True,
        help="Export from sales Cube(Our lines in YTD price effect, split EXW-DEL).",
        metavar="path",
        dest="sales",
    )
    parser.add_argument(
        "--mdm",
        required=True,
        help="Export from mdm API.",
        metavar="path",
        dest="mdm",
    )
    parser.add_argument(
        "--LP",
        required=True,
        help="List Price export from 006 list price powerBI report",
        metavar="path",
        dest="lp",
    )
    parser.add_argument(
        "--so",
        required=True,
        help="legal entity - Sales Organization extract 006 price conditions powerBI report",
        metavar="path",
        dest="so",
    )
    parser.add_argument(
        "--zcpr",
        required=True,
        help="Product price condirions extract 006 price conditions powerBI report",
        metavar="path",
        dest="zcpr",
    )
    parser.add_argument(
        "--stdcosts",
        required=True,
        help="Standard costs extract 023 Standard costs history extract from powerBI report",
        metavar="path",
        dest="stdcosts",
    )
    parser.add_argument(
        "--sapcosts",
        required=True,
        help="Standard costs extract from SAP material list",
        metavar="path",
        dest="sapcosts",
    )
    # Parse the command line args
    settings = parser.parse_args()
    # Run code
    main(settings)
