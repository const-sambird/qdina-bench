class Generator:
    def __init__(self):
        pass

    def generate(self, rng_seed: int | None = None):
        '''
        Generate the requisite query and table data, seeding with the provided
        value. If None is passed, then the Generator will create its own value.
        '''
        raise NotImplementedError
    
    def load_database(self):
        '''
        Load the database with the generated table data.
        '''
        raise NotImplementedError

    def read_data(self) -> list[str]:
        '''
        Reads the generated query data into memory and returns it
        as a list of strings.

        :returns: the generated queries
        '''
        raise NotImplementedError
