import pyodbc
import sys
import os
import datetime
from  metaphone import doublemetaphone
import logging
logging.basicConfig(level=logging.DEBUG)

conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER={LVM-04};'
        'DATABASE=master;'
        'Trusted_Connection=yes;'
        'MARS_Connection=Yes;'
        'ConnectionPoolSize=500;')
cur = conn.cursor()

def get_databases():
    cur.execute("SELECT name FROM sys.databases WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb');")
    return [row[0] for row in cur.fetchall()]

from metaphone import doublemetaphone

def generate_similar_words(input_word, max_distance=1):
    primary_code, _ = doublemetaphone(input_word)
    similar_words = set()

    # Same length variations (original logic)
    for i in range(len(input_word)):
        for char in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ":
            if char != input_word[i]:
                variation = input_word[:i] + char + input_word[i+1:]
                variation_code, _ = doublemetaphone(variation)
                if variation_code == primary_code:
                    similar_words.add(variation)

    # One character longer variations
    for i in range(len(input_word) + 1):
        for char in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ":
            variation = input_word[:i] + char + input_word[i:]
            variation_code, _ = doublemetaphone(variation)
            if variation_code == primary_code:
                similar_words.add(variation)

    # One character shorter variations
    for i in range(len(input_word)):
        variation = input_word[:i] + input_word[i+1:]
        variation_code, _ = doublemetaphone(variation)
        if variation_code == primary_code:
            similar_words.add(variation)

    similar_words.discard(input_word)
    return similar_words


# Golbal dic
db_table_pairs = {}

def level1(keyword, tag):
    try:
        level_1_results = []
        email_formats = []

        
        if tag.lower() == 'email':
            last_at = keyword.rfind('@')    # Find the last '@' in the email content
            email_cont = keyword[:last_at]
            domain = keyword[last_at+1:]
            # Generate different email formats
            email_formats = [
                f"www.{email_cont}@{domain}",
                f"{email_cont}@{domain}"
            ]
            email_patterns = ['at', '{at}', '[at]', '(at)']
            for pattern in email_patterns:
                email_formats.append(f"www.{email_cont}{pattern}{domain}")
                email_formats.append(f"{email_cont}{pattern}{domain}")
        
        with open("results_1.txt", "w", encoding="utf-8") as file:
            i_counter = 1  # Initialize i_counter outside the loop
            
            for db in get_databases():
                cur.execute(f"USE {db};")
                if tag.lower() == 'email':
                    for emaill in email_formats:
                        keyword = emaill
                        query = f"SELECT NAME_TABLE, NAME_COLUMN FROM dbo.TAG WHERE TAG LIKE ?;"
                        tables_columns = cur.execute(query, f"%{tag}%").fetchall()
                        
                        for table, column in tables_columns:
                            query = f"SELECT * FROM dbo.{table} WHERE {column} COLLATE SQL_Latin1_General_CP1_CS_AS LIKE '{keyword}';"
                            file.write(query + "\n")
                            
                            query_res = cur.execute(query).fetchall()
                            if query_res:
                                rows = [dict(zip([column[0] for column in cur.description], row)) for row in query_res]
                                db_table_pairs.setdefault((db, table), []).extend(rows)  # Add rows to the dictionary
                                
                                for row in rows:
                                    level_1_result = (f"L1R{i_counter}:", db, 'dbo', table, row)
                                    if level_1_result not in level_1_results:   # Handle Duplicated Values
                                        level_1_results.append(level_1_result)
                                        output = f"{level_1_result}"
                                        file.write(output + "\n")
                                        i_counter += 1  # Increment i_counter here
                elif 'name' in tag.lower():
                    # Get the similar words
                    similar_words = generate_similar_words(keyword)
                    
                    # Search for all similar words
                    for db in get_databases():
                        cur.execute(f"USE {db};")
                        query = f"SELECT NAME_TABLE, NAME_COLUMN FROM dbo.TAG WHERE TAG LIKE ?;"
                        tables_columns = cur.execute(query, f"%{tag}%").fetchall()
                        
                        for table, column in tables_columns:
                            for similar_word in similar_words:
                                query = f"SELECT * FROM dbo.{table} WHERE {column} COLLATE SQL_Latin1_General_CP1_CS_AS LIKE '{similar_word}';"
                                file.write(query + "\n")
                                
                                query_res = cur.execute(query).fetchall()
                                if query_res:
                                    rows = [dict(zip([column[0] for column in cur.description], row)) for row in query_res]
                                    db_table_pairs.setdefault((db, table), []).extend(rows)  # Add rows to the dictionary
                                    
                                    for row in rows:
                                        level_1_result = (f"L1R{i_counter}", db, 'dbo', table, row)
                                        if level_1_result not in level_1_results:    # Handle Duplicated Values
                                            level_1_results.append(level_1_result)
                                            output = f"{level_1_result}"
                                            file.write(output + "\n")
                                            i_counter += 1  # Increment i_counter here
                else:
                    query = f"SELECT NAME_TABLE, NAME_COLUMN FROM dbo.TAG WHERE TAG LIKE ?;"
                    tables_columns = cur.execute(query, f"%{tag}%").fetchall()
                    
                    for table, column in tables_columns:
                        query = f"SELECT * FROM dbo.{table} WHERE {column} COLLATE SQL_Latin1_General_CP1_CS_AS LIKE '{keyword}';"
                        file.write(query + "\n")
                        
                        query_res = cur.execute(query).fetchall()
                        if query_res:
                            rows = [dict(zip([column[0] for column in cur.description], row)) for row in query_res]
                            db_table_pairs.setdefault((db, table), []).extend(rows)  # Add rows to the dictionary
                            
                            for row in rows:
                                level_1_result = (f"L1R{i_counter}", db, 'dbo', table, row)
                                if level_1_result not in level_1_results:    # Handle Duplicated Values
                                    level_1_results.append(level_1_result)
                                    output = f"{level_1_result}"
                                    file.write(output + "\n")
                                    i_counter += 1  # Increment i_counter here
        
        print('level1 finished')
        return level_1_results
    
    except Exception as e:
        print(f"An error occurred in level1: {e}")
        return []

def get_all_levels(initial_results, tag,max_level=3):
    try:
        conn_str = (
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER={LVM-04};'
        'DATABASE=master;'
        'Trusted_Connection=yes;'
        'MARS_Connection=Yes;'
        'ConnectionPoolSize=500;')
 
        conn = pyodbc.connect(conn_str)
        cur = conn.cursor()

        results = [initial_results]
        level = 2
        seen_results = set()  # Set to store unique results

        # Process level 1 results
        with open("all_levels_str_results.txt", "w", encoding="utf-8") as file:
            unique_level1_results = set()
            for level_output, db, schema, table, row in results[0]:
                for column, value in row.items():
                    if value:
                        result = f"{db}, '{schema}', {table}, {row}"
                        if result not in unique_level1_results:
                            unique_level1_results.add(result)
                            file.write(result + "\n")
                            seen_results.add(result)

        while level <= max_level:
            with open(f"results_level_{level}.txt", "w", encoding="utf-8") as file:
                with open(f"queries_level_{level}.txt", "w", encoding="utf-8") as query_file:
                    next_level_results = []
                    i = 1  # Initialize i for rows
                    for level_output, db, schema, table, row in results[-1]:
                        # Execute the query to get the column names and tags
                        cur.execute(f"USE {db};")
                        query = f"SELECT NAME_COLUMN, TAG FROM dbo.TAG WHERE NAME_TABLE = ? AND TAG NOT LIKE ? AND TAG NOT LIKE '%INDEX%';"
                        columns_tags = cur.execute(query, table, f"%{tag}%").fetchall()

                        # Search for related tables and columns
                        for column, tags in columns_tags:
                            new_keyword = row.get(column, '')
                            if new_keyword:

                                
                                if 'NOT_UNIQUE' not in tags:
                                    new_tags = tags.split(',')
                                    for new_tag in new_tags:
                                        if new_tag.lower() == 'email':
                                            # Handle email-related logic
                                            last_at = new_keyword.rfind('@')
                                            email_cont = new_keyword[:last_at]
                                            domain = new_keyword[last_at+1:]
                                            email_formats = [
                                                f"www.{email_cont}@{domain}",
                                                f"{email_cont}@{domain}"
                                            ]
                                            email_patterns = ['at', '{at}', '[at]', '(at)']
                                            for pattern in email_patterns:
                                                email_formats.append(f"www.{email_cont}{pattern}{domain}")
                                                email_formats.append(f"{email_cont}{pattern}{domain}")

                                            for db in get_databases():
                                                cur.execute(f"USE {db};")
                                                for emaill in email_formats:
                                                    new_keyword = emaill
                                                    query = f"SELECT NAME_TABLE, NAME_COLUMN FROM dbo.TAG WHERE TAG LIKE ?;"
                                                    L2tables_columns = cur.execute(query, f"%{new_tag}%").fetchall()
                                                    for l2table, l2column in L2tables_columns:
                                                        query = f"SELECT * FROM dbo.{l2table} WHERE {l2column} COLLATE SQL_Latin1_General_CP1_CS_AS LIKE '%{new_keyword}%' AND {l2column} NOT LIKE 'nan';"
                                                        query_file.write(str(query + "\n"))
                                                        
                                                        try:
                                                            query_res = cur.execute(query).fetchall()
                                                            if query_res:
                                                                l2new_rows = [dict(zip([column[0] for column in cur.description], row)) for row in query_res]
                                                                db_table_pairs.setdefault((db, l2table), []).extend(l2new_rows)  # Add rows to the dictionary

                                                                for l2new_row in l2new_rows:
                                                                    l2_result = (f"{level_output} {column} L{level}R{i}:", db, schema, l2table, l2new_row)
                                                                    result_str = f"{db}.{schema}.{l2table}.{','.join(str(v) for v in l2new_row.values())}"
                                                                    if result_str not in seen_results:
                                                                        seen_results.add(result_str)
                                                                        next_level_results.append(l2_result)
                                                                        output = f"{l2_result}"
                                                                        file.write(output + "\n")
                                                                        i += 1  # Increment i here
                                                        except pyodbc.ProgrammingError as e:
                                                            print(f"Error executing query on {db}.{l2table}: {e}")
                                            email_formats.clear()
                                        else:
                                            for db in get_databases():
                                                cur.execute(f"USE {db};")
                                                query = f"SELECT NAME_TABLE, NAME_COLUMN FROM dbo.TAG WHERE TAG LIKE ?;"
                                                L2tables_columns = cur.execute(query, f"%{new_tag}%").fetchall()
                                                for l2table, l2column in L2tables_columns:
                                                    query = f"SELECT * FROM dbo.{l2table} WHERE {l2column} COLLATE SQL_Latin1_General_CP1_CS_AS LIKE '%{new_keyword}%'AND {l2column} NOT LIKE 'nan';"
                                                    query_file.write(str(query + "\n"))
                                                    
                                                    try:
                                                        query_res = cur.execute(query).fetchall()
                                                        if query_res:
                                                            l2new_rows = [dict(zip([column[0] for column in cur.description], row)) for row in query_res]
                                                            db_table_pairs.setdefault((db, l2table), []).extend(l2new_rows)  # Add rows to the dictionary

                                                            for l2new_row in l2new_rows:
                                                                l2_result = (f"{level_output} {column} L{level}R{i}:", db, schema, l2table, l2new_row)
                                                                result_str = f"{db}.{schema}.{l2table}.{','.join(str(v) for v in l2new_row.values())}"
                                                                if result_str not in seen_results:
                                                                    seen_results.add(result_str)
                                                                    next_level_results.append(l2_result)
                                                                    output = f"{l2_result}"
                                                                    file.write(output + "\n")
                                                                    i += 1  # Increment i here
                                                    except pyodbc.ProgrammingError as e:
                                                        print(f"Error executing query on {db}.{l2table}: {e}")

                     # Read existing results from the file
                    with open("all_levels_str_results.txt", "r", encoding="utf-8") as file:
                        existing_results = {line.strip() for line in file}

                    # Write unique results from next_level_results to the file
                    with open("all_levels_str_results.txt", "a", encoding="utf-8") as file:
                        to_remove = []
                        for result in next_level_results:
                            result_str = f"{result[1]}, '{result[2]}', {result[3]}, {result[4]}"
                            #print(result_str)
                            if result_str not in existing_results:
                                existing_results.add(result_str)
                                output = f"{result_str}"
                                file.write(output + "\n")
                            else:
                                to_remove.append(result)
                        
                        # Remove duplicate results from next_level_results
                        for result in to_remove:
                            next_level_results.remove(result)

                    # Check if there are no new results for the current level
                    if not next_level_results:
                        print(f"No new results found at level {level}. Stopping the loop.")
                        break

                    # Append the current level results to the overall results
                    results.append(next_level_results)
                    print(f"Level {level} finished")                   
                    level += 1

        print(f"All levels up to {level} completed.")
        return results

    except pyodbc.ProgrammingError as e:
        print(f"A programming error occurred: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None
    
def main():
    
    keyword, tag = [value.strip() for value in input("Please enter the keyword and tag (separated by a comma): ").split(',')]
    # Get the current date and time
    now = datetime.datetime.now()
    formatted_datetime = now.strftime("%A, %B %d, %Y %I:%M %p")

    with open("all results.txt", "w", encoding="utf-8") as all_files: 
        all_files.write(f"Start date and time: {formatted_datetime}\n")
        all_files.write(f"The Keyword that user enterd: {keyword}\n")
        all_files.write(f"***********************************************\n")

    
    res_full = []
    #query_full = []
    res1 = level1(keyword, tag)
    if res1:
        res_full.extend([res1])
        #query_full.extend(query_full_1)
    else:
        print("The `level1()` function returned an empty list.")

    res_oth = get_all_levels(res1, tag)
    if res_oth:
        res_full.extend(res_oth)
        #query_full.extend(query_full_2)
    else:
        print("The `get_all_levels()` function returned an empty list.")
 
    #Write the whole results in the all results.txt pair of(db,table)                                
    unique_results = {}

    try: 
        with open("all results.txt", "a", encoding="utf-8") as all_files:
            for (db, table), rows in db_table_pairs.items():
                key = (db, table)
                if key not in unique_results:
                    all_files.write(f"{db},{table}\n")
                    unique_results[key] = []            
                for row in rows:
                    row_str = ', '.join(f"{key}: {value}" for key, value in row.items())
                    if row_str not in unique_results[key]:
                        all_files.write(f"{row_str}\n")
                        unique_results[key].append(row_str)         
                all_files.write("\n")
            
            print(f"NO new rows detected and the program finished")
            end_time = datetime.datetime.now().strftime("%A, %B %d, %Y %I:%M %p")
            all_files.write(f"End date and time: {end_time}\n")
    except Exception as e:
        print(f"An error occurred: {e}")                
        
        end_time = datetime.datetime.now().strftime("%A, %B %d, %Y %I:%M %p")
        all_files.write(f"End date and time: {end_time}\n")

if __name__ == "__main__":
    main()
