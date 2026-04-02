import pandas as pd
import os
import sys
import csv

sys.path.append(os.environ["CODE"])
import adlib_v3 as adlib

if __name__ == "__main__":
    results_csv = []
    file = sys.argv[1]
    output_file = sys.argv[2]
    amend_df = pd.read_csv(file)
    df = amend_df[["filepath","priref", "title.article","title","title.language","title.type","title.article_cid","title_cid","title.language_cid","title.type_cid"]]
    for index, row in df.iterrows():
         print(row['priref'])
         df_row_dict = row[[
    "title.article","title","title.language","title.type",
    "title.article_cid","title_cid","title.language_cid","title.type_cid"
            ]].to_dict()
         df_row_dict = {k: "" if pd.isna(v) else v for k,v in df_row_dict.items()}
         search = f'priref="{row["priref"]}"'
         hit, record = adlib.retrieve_record(os.environ.get("CID_API4"), "manifestations", search, "1")
         part_reference_result = adlib.retrieve_field_name(record[0], 'parts_reference.lref')
         results_one = {"priref": part_reference_result[0]}

         final = {**results_one, **(df_row_dict)}
         print(f"Final: {final}")

         results_csv.append(final)
    print("writing results to csv...")
    with open(output_file, "w+", newline="") as file:
          fieldnames = results_csv[0].keys()
          writer = csv.DictWriter(file, fieldnames=fieldnames)
          writer.writeheader()
          writer.writerows(results_csv)





