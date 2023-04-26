from helpers.write_csv import WriteCSV


class DatabaseHelper:
    def get_columns_type(self, cur, table_name):
        cur.execute(
            f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}'")
        details = dict()
        for columns in cur.fetchall():
            if columns[1] == 'double precision':
                details[columns[0]] = 'float'
            elif columns[1] == 'timestamp with time zone':
                details[columns[0]] = 'timestamptz'
            else:
                details[columns[0]] = columns[1]
        return details

    def get_columns_type_schema(self, statement):
        details = dict()
        for column in statement:
            if column != '(' or column != ')':
                if ',' in column:
                    column = column.replace(',', '').split(' ')
                else:
                    column = column.split(' ')
                if len(column) > 4 and column[0] == '':
                    if column[5].lower() == 'varchar(100)[]':
                        if 'primary' in column[4].lower():
                            continue
                        details[column[4]] = 'ARRAY'
                    else:
                        if 'primary' in column[4].lower():
                            continue
                        details[column[4]] = column[5].lower()
                else:
                    if 'primary' in column[0].lower():
                        continue
                    details[column[0].replace('\t', '')] = column[1].lower()
        return details

    def check_valid_schema(self, table_name, source, destination):
        result = list()
        source_columns = list()
        source_types = list()
        destination_type = list()
        matching = list()
        data = dict()
        for item in source.items():
            if item[0] in destination.keys():
                column_type = destination[item[0]]
                if item[1] == column_type:
                    source_columns.append(item[0])
                    source_types.append(item[1])
                    destination_type.append(column_type)
                    matching.append(True)
                    # res = f'column {item[0]} with type {item[1]} from Schema file and type {column_type} from Database Table is Match'
                    # result.append(res)
                else:
                    source_columns.append(item[0])
                    source_types.append(item[1])
                    destination_type.append(column_type)
                    matching.append(False)
                    # res = f"column {item[0]} with type {item[1]} from Schema file and type {column_type} from Database Table is doesn't Match"
                    # result.append(res)
            else:
                res = f"column {item[0]} with type {item[1]} is doesn't exist in Database Table"
                result.append(res)
        data['SourceColumns'] = source_columns
        data['SourceType'] = source_types
        data['DestinationType'] = destination_type
        data['Matching'] = matching
        WriteCSV().build_csv(table_name, data)
        # with open(f'check_result/{table_name}.txt', 'w+') as f:
        #     for res in result:
        #         f.write(f'{res}\n')
