
# MapReduce Longest Word Example

This project implements a MapReduce solution to identify the longest word given in the input file using Hadoop's MapReduce framework to efficiently process large volumes of text data.

## Components

- **Mapper Class**: Processes input text, splits it into individual words, and maps each word with its length.
- **Reducer Class**: Aggregates the word-length pairs produced by the Mapper and determines the longest word in the dataset.
- **Driver Class**: Configures and runs the MapReduce job, linking the Mapper and Reducer classes and managing job execution.

## How It Works

1. **Mapping**: The Mapper extracts words from the input data and then splits it into key value pairs
2. **Reducing**: The Reducer receives the word-length pairs, compares lengths, and identifies the longest word. Output is written to the file in the driver
3. **Execution**: The Driver sets up and executes the MapReduce job, managing the overall process and output.

## Technologies Used

- **Hadoop**: For distributed processing of the dataset.
- **MapReduce**: Framework for parallel data processing.

This project demonstrates a practical use of MapReduce for text analysis and showcases the efficiency of distributed computing in handling large-scale data processing tasks.
