class QueryServices :
    Create_Table = (
        """
        create table IF NOT EXISTS {Schema}.{Table} (
            {Columns}
        );
        """
    )

    Create_Index = (
        """
        CREATE INDEX IF NOT EXISTS idx_{Table} 
                    ON {Schema}.{Table} ({Columns});
        """
    )

    Create_FK = (
        """
        ALTER TABLE {Table} 
        ADD CONSTRAINT fk_{constraint_name} 
        FOREIGN KEY ({fk_columns}) 
        REFERENCES {parent_table}({fk_parent_key_columns});
        """
    )

    truncate_table = (
        """
        truncate {table}
        """
    )

    Import_Csv = (
        """
        COPY {Table}({Columns})
        FROM '{Path_File}'
        DELIMITER ','
        CSV HEADER;
        """
    )

    Alter_Column = (
        """
        {statement}
        """
    )

