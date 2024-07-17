Profiler is an advanced data mining and relationship mapping tool designed to build comprehensive profiles for individuals based on a search query. It explores multiple databases and constructs a graph of relationships between the searched person and other related individuals.
Features

    Multi-level, cross-database search capability
    Intelligent email format handling
    Relationship graph construction
    Duplicate result prevention
    Detailed logging and result storage

How It Works
The Profiler uses a multi-tiered approach to gather and connect information:

   1. Initial Search: Starts with a keyword and tag provided by the user.
   2. Level 1 Search: Performs an initial database query to find direct matches.
   3. Multi-level Search: Explores related information up to a specified depth (default 3 levels).
   4. Result Compilation: Aggregates and deduplicates results from all levels.
   5. Profile Building: Constructs a profile based on the gathered information.

Key Features Explained

    Multi-level Search: The tool performs searches at multiple levels, starting from the initial query and expanding based on related information found in each level.
    Cross-database Exploration: Searches are conducted across multiple databases, allowing for comprehensive data gathering.
    Intelligent Email Handling: The system recognizes various email formats and patterns, enhancing the search capabilities for email-related information.
    Deduplication: A robust mechanism is in place to prevent duplicate entries, ensuring the profile contains unique information.
    Error Handling: The code includes error catching and logging for database operations, enhancing reliability.

Usage

    Run the main script.
    Enter a keyword and tag when prompted (e.g., "john.doe@example.com, email").
    The system will perform the multi-level search and compile results.
    Results are stored in various text files for further analysis.

Output Files

    all_levels_str_results.txt: Contains all unique results across all search levels.
    results_level_{level}.txt: Stores results for each specific search level.
    queries_level_{level}.txt: Logs the queries executed at each level.
    all results.txt: Compiles the final profile information.

Future Enhancements

    Implement a graphical user interface for easier interaction.
    Add visualization capabilities for the relationship graph.
    Enhance search algorithms for better performance on large datasets.
    Implement more advanced natural language processing for improved search relevance.

