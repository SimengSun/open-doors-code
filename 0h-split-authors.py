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

    # query database to get the author_ids whose number of works > 200
    author_works_count = sql.execute_dict("""SELECT author_id, COUNT(item_id) AS a FROM item_authors 
                                             GROUP BY author_id ORDER BY a DESC;""")
    # for each author
    for row in tqdm(author_works_count):

        this_author_id = row['author_id']
        if row['a'] > 200:
            # decide how many new author_ids to insert
            num_new_ids = row['a'] // 200

            # retrieve the corresponding author row
            this_author = sql.execute_dict(f"""SELECT * FROM authors 
                                             WHERE id = {row['author_id']};""")[0]

            new_author_id = sql.execute_dict(f"""SELECT MAX(id) FROM authors;""")[0]['MAX(id)'] + 1

            # insert new ids to `authors`
            new_ids = []    
            for ai in range(num_new_ids):
                # insert new author_ids
                this_author['id'] = new_author_id + ai
                new_ids.append(this_author['id'])
                sql.execute(f"""INSERT INTO `authors` VALUES ({', '.join(["'"+str(x)+"'" for x in this_author.values()])}); """)

            # retrieve the list of story_ids by this author
            stories_this_author = sql.execute_dict(f"""SELECT * FROM item_authors 
                                             WHERE author_id = {this_author_id};""")

            # split stories into group 200 and from after 200 rows
            stories_this_author = stories_this_author[200:]

            for ni, new_id in enumerate(new_ids):
                stories_for_new_ids = stories_this_author[ni*200:min((ni+1)*200, len(stories_this_author))]
                story_id_to_be_updated = [row['item_id'] for row in stories_for_new_ids]
                # update the item(story)_ids in the `author_items` table
                for this_item_id in story_id_to_be_updated:
                    sql.execute(f"""
                        UPDATE item_authors SET `author_id` = '{new_id}' where item_id = {this_item_id}; 
                    """)

        else:
            break

