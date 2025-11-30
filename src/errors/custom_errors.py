class CantMapSelectedTables(Exception):
    def __init__(self, unmapped_tables=None):
        self.unmapped_tables = unmapped_tables
        super().__init__(f"Cannot map selected tables: {', '.join(self.unmapped_tables)}")