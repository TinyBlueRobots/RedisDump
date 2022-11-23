A simple tool to dump a redis db to text files

```
USAGE: RedisDump [--help] [--connectionstring <string>] [--database <int>] [--output <string>] [--parallelism <int>]

OPTIONS:

    --connectionstring <string>
                          connection string (defaults to localhost)
    --database <int>      database number (defaults to 0)
    --output <string>     output directory (defaults to currentDirectory/database)
    --parallelism <int>   number of parallel tasks (defaults to 1)
    --help                display this list of options.
```