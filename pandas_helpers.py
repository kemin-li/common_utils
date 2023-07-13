"""Utility functions for pandas
Created by Kemin Li
"""
import pandas as pd

def output_to_excel(data_dict, output_path, formats=None, index=False):
    """
    column_formats = dict(
        company={"width": 50},
        website={"width": 20},
        revenue={"width": 16, "cell_format": {"num_format": "#,##0"}}
    )
    output_to_excel(sample_dict, "/mnt/tmp/data_samples.xlsx", column_formats)
    """
    writer=pd.ExcelWriter(output_path)
    workbook = writer.book
    for sheet, data in data_dict.items():
        data.to_excel(writer, sheet_name=sheet, index=index)
        if formats is not None:
            worksheet = writer.sheets[sheet]
            for ind, col in enumerate(data.columns):
                if col in formats:
                    col_formats = {k: v for k, v in formats[col].items()}  # recreate a format obj
                    if "cell_format" in col_formats:
                        col_formats["cell_format"] = workbook.add_format(col_formats["cell_format"])
                    if index:
                        ind += 1
                    worksheet.set_column(ind, ind, **col_formats)
    writer.save()
    writer.close()
    return

