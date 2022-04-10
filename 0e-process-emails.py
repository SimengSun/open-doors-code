# encoding: utf-8
import os
import re
import pdb

from tqdm import tqdm
from pymysql import escape_string

from shared_python.Args import Args
from shared_python.Chapters import Chapters
from shared_python.FinalTables import FinalTables
from shared_python.Sql import Sql
from shared_python.Tags import Tags

if __name__ == "__main__":
    args_obj = Args()
    args = args_obj.args_for_05()
    log = args_obj.logger_with_filename()
    sql = Sql(args, log)
    tags = Tags(args, sql, log)
    final = FinalTables(args, sql, log)
    chaps = Chapters(args, sql, log)

    email_regex = "[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z0-9]{2,4}"

    # query final db, get rows where notes has @ symbol and match email string
    at_rows = sql.execute_dict("""SELECT * From stories where notes like '%%@%%'""")

    # print number of returned entries
    print(f"{len(at_rows)} rows in stories has @ in notes")

    # for each returned entry, manually decide whether to replace with redacted email
    for row in tqdm(at_rows):
        emails = re.findall(email_regex, row['notes'])
        for email in emails:
            print(f"Replace {email} with [email redacted]? (Y/N)")
            res = input()
            if res in ["Y", "y", ""]:
                row['notes'] = row['notes'].replace(email, "[email redacted]")
                print(escape_string(row['notes']))
                sql.execute(f"""
                        UPDATE stories SET notes = '{escape_string(row['notes'])}' where id = {row['id']}; 
                    """)

    # query final db, get rows where story text has @ symbol and match email string
    at_rows = sql.execute_dict("""SELECT * From chapters where `text` like '%%@%%'""")

    # print number of returned entries
    print(f"{len(at_rows)} rows in stories has @ in notes")

    # for each returned entry, manually decide whether to replace with redacted email
    for row in tqdm(at_rows):
        emails = re.findall(email_regex, row['text'])
        for email in emails:
            print(f"Replace {email} with [email redacted]? (Y/N)")
            res = input()
            if res in ["Y", "y", ""]:
                row['text'] = row['text'].replace(email, "[email redacted]")
                row['text'] = row['text'].replace("%", "%%") # to escape the python format symbol
                sql.execute(f"""
                        UPDATE chapters SET `text` = '{escape_string(row['text'])}' where id = {row['id']}; 
                    """)
