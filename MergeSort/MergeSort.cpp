#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <algorithm>
#include <queue>
#include <stack>
#include <cmath>

using namespace std;

const int b = 250;  // number of records in one buffer
const int n = 20;    // number of buffers

const int block_size = b * n;   // run
const int page_size = INT32_MAX;

int tmp_file_counter = 0;
int sorting_phases_counter = 0;
int read_operations_counter = 0;
int write_operations_counter = 0;
int merge_operations_counter = 0;
int expected_drive_operations = 0;

double calculate_drive_operations(int N, int b, int n)
{
    double log_n = log(n) / log(2);       // lg(n)
    double log_N_over_b = log(static_cast<double>(N) / b) / log(2); // lg(N/b)

    double result = 2 * (static_cast<double>(N) / (b * log_n)) * log_N_over_b;
    return result;
}


struct Record
{
    vector<short> numbers;
    short length = 0;
    vector<short> sorted;

    void print_sorted()
    {
        for (int i = 0; i < sorted.size(); i++)
        {
            cout << sorted[i] << " ";
        }
    }
};

ostream& operator<<(ostream& os, const Record& record)
{
    for (int i = 0; i < record.numbers.size(); i++)
    {
        os << record.numbers[i] << " ";
    }
    return os;
}

bool compare_records(const Record& a, const Record& b)
{
    if (a.sorted.size() == b.sorted.size())
    {
        for (int i = 0; i < a.sorted.size(); i++)
        {
            if (a.sorted[i] != b.sorted[i])
            {
                return a.sorted[i] > b.sorted[i];
            }
        }
    }
    return a.length > b.length;
}

// struct to simplify heap-sort
struct Run {
    ifstream stream;
    int level;

    bool operator>(const Run& other) const {
        return level > other.level;
    }

    bool eof() const {
        return stream.eof();
    }
};

bool compare_runs(const Run& a, const Run& b)
{
    return a.level < b.level;
}

void sort_block(vector<Record>* block)
{
    sort(block->begin(), block->end(), compare_records);
    sorting_phases_counter++;
}

vector<string> page_read(ifstream& file, int max_records_number)
{
    vector<string> page_data;
    streampos position = file.tellg();
    string strbuf;
    int page_data_capacity = 0;
    while (page_data_capacity <= page_size && getline(file, strbuf))
    {
        page_data_capacity += strbuf.length();
        page_data.push_back(strbuf);

        if (page_data_capacity > page_size || page_data.size() > max_records_number)
        {
            file.seekg(position);
            file.clear();
            page_data.pop_back();
            break;
        }

        position = file.tellg();
    }

    read_operations_counter++;
    return page_data;
}


vector<string> page_read_one(ifstream& file)
{
    streampos position = file.tellg();
    vector<string> page = page_read(file, 1);
    file.clear();
    file.seekg(position);
    string buf;
    getline(file, buf);
    return page;
}

void page_write(ofstream& file, vector<Record> records)
{
    for (int i = 0; i < records.size(); i++)
    {
        file << records[i] << endl;
    }

    write_operations_counter++;
}

Record process_record(const string& line)
{
    Record record;
    record.length = line.length();
    for (int i = 0; i < line.length(); i += 2)
    {
        record.numbers.push_back(line[i] - '0');
    }

    record.sorted = record.numbers;
    sort(record.sorted.rbegin(), record.sorted.rend());

    return record;
}

vector<Record> block_read(ifstream& file, int block_size)
{
    vector<Record> records;
    int count = 0;

    while (count < block_size)
    {
        vector<string> page = page_read(file, block_size - count);
        for (auto recordStr : page)
        {
            if (recordStr == "\0")
            {
                break;
            }

            records.push_back(process_record(recordStr));
            count++;
        }

        if (file.eof())
        {
            break;
        }
    }


    return records;
}

queue<Record> block_read_q(ifstream& file, int block_size)
{
    queue<Record> records;
    int count = 0;

    while (count < block_size)
    {
        vector<string> page = page_read(file, block_size - count);
        for (auto recordStr : page)
        {
            if (recordStr == "\0")
            {
                break;
            }

            records.push(process_record(recordStr));
            count++;
        }

        if (file.eof())
        {
            break;
        }
    }

    return records;
}

void block_write(ofstream& file, vector<Record>& buffer)
{
    while (!buffer.empty())
    {
        vector<Record> page;
        int page_capacity = 0;
        while (page_capacity < page_size && !buffer.empty())
        {
            Record record = buffer[buffer.size() - 1];
            buffer.pop_back();
            page_capacity += record.length * 2;
            page.push_back(record);
        }

        page_write(file, page);
    }
}

string merge_buffers(vector<ifstream>& buffer_files)
{
    string output_path = "data/tmp/tmp_file" + to_string(tmp_file_counter++) + ".txt";
    ofstream output_file(output_path);

    // struct to simplify heap-sort
    struct MergeRecord {
        Record record;
        int fileIndex;

        bool operator>(const MergeRecord& other) const {
            return compare_records(record, other.record);
        }
    };

    int buffers_number = buffer_files.size();
    queue<Record>* buffers = new queue<Record>[buffers_number];
    vector<Record> output_buffer;
    priority_queue<MergeRecord, vector<MergeRecord>, greater<MergeRecord>> minHeap;

    // fill buffers and heap
    for (int i = 0; i < buffers_number; i++)
    {
        buffers[i] = block_read_q(buffer_files[i], b);
        if (!buffers[i].empty())
        {
            minHeap.push({ buffers[i].front(), i });
            buffers[i].pop();
        }
    }

    while (!minHeap.empty())
    {
        MergeRecord top = minHeap.top();
        output_buffer.push_back(top.record);
        int last_buffer = top.fileIndex;
        minHeap.pop();

        // if output buffer is full, write it on drive
        if (output_buffer.size() == b)
        {
            reverse(output_buffer.begin(), output_buffer.end());
            block_write(output_file, output_buffer);
            output_buffer.clear();
        }

        // get next item from last used buffer
        if (!buffers[last_buffer].empty())
        {
            minHeap.push({ buffers[last_buffer].front(), last_buffer });
            buffers[last_buffer].pop();
        }

        // refill buffers
        for (int i = 0; i < buffers_number; i++)
        {
            if (buffers[i].empty() && !buffer_files[i].eof())
            {
                buffers[i] = block_read_q(buffer_files[i], b);
            }
        }
    }

    output_file.close();
    delete[] buffers;
    merge_operations_counter++;
    return output_path;
}

void merge_runs(vector<string>& tmp_files_paths, const string& output_file)
{
    ofstream output_stream(output_file);
    vector<Run> runs;
    vector<ifstream> buffers;
    int current_priority_level = 0;
    for (auto& file_path : tmp_files_paths)
    {
        runs.push_back({ ifstream(file_path), current_priority_level++ });
    }

    while (runs.size() > 1)
    {
        sort(runs.begin(), runs.end(), compare_runs);

        // get next runs to merge
        for (int i = 0; i < n - 1; i++)
        {
            if (runs.size() > 0)
            {
                buffers.push_back(move(runs[0].stream));
                runs[0].stream.close();
                runs.erase(runs.begin());
            }
        }

        // create new, bigger run
        string new_tmp_path = merge_buffers(buffers);
        tmp_files_paths.push_back(new_tmp_path);
        runs.push_back({ ifstream(new_tmp_path), current_priority_level++ });
    }
}


void large_buffer_sort(int N)
{
    tmp_file_counter = 0;
    sorting_phases_counter = 0;
    read_operations_counter = 0;
    write_operations_counter = 0;
    merge_operations_counter = 0;
    expected_drive_operations = calculate_drive_operations(N, b, n);

    string input_file = "data/input_" + to_string(N) + ".txt";
    string output_file = "data/output.txt";
    string tmp_files_path = "data/tmp/tmp_file";
    vector<string> tmp_files_paths;

    ifstream input(input_file);
    if (!input.is_open())
    {
        cerr << "Nie można otworzyć pliku wejściowego.\n";
        return;
    }

    while (!input.eof())
    {
        vector<Record> buffer = block_read(input, block_size);

        if (buffer.empty())
            break;

        sort_block(&buffer);

        string tmp_file_path = tmp_files_path + to_string(tmp_file_counter++) + ".txt";
        tmp_files_paths.push_back(tmp_file_path);

        ofstream tmp_stream(tmp_file_path);
        if (!tmp_stream.is_open())
        {
            cerr << "Nie można otworzyć pliku tymczasowego.\n";
            return;
        }

        block_write(tmp_stream, buffer);
        tmp_stream.close();
    }

    input.close();

    merge_runs(tmp_files_paths, output_file);

    tmp_files_paths.pop_back();
    for (const string& file : tmp_files_paths)
    {
        remove(file.c_str());
    }

    cout << "Liczba przeprowadzonyh opracji:" << endl;
    cout << "Liczba faz sortowania: " << sorting_phases_counter << endl;
    cout << "Liczba odczytow z dysku: " << read_operations_counter << endl;
    cout << "Liczba zapisow na dysk: " << write_operations_counter << endl;
    cout << "Liczba operacji mergowania: " << merge_operations_counter << endl;

    cout << "Calkowita liczba operacji dyskowych: " << read_operations_counter + write_operations_counter << endl;
    cout << "Oczekiwana liczba operacji dyskowych: " << expected_drive_operations << endl << endl;;
}

int main()
{
    string results_path = "data/result.csv";
    ofstream results_file = ofstream(results_path);
    
    results_file << "N,n,b,sorting_phases,read_operations,write_operations,merge_operations,drive_operations,expected_drive_operations\n";

    int Ns[] = {10, 100, 1000, 10000, 100000, 1000000};
    for (int N : Ns)
    {
        printf("Sortowanie dla parametrow: N=%d, n=%d, b=%d.\n", N, b, n);
        large_buffer_sort(N);

        results_file << N << "," << n << "," << b << "," << "," << sorting_phases_counter << "," <<
            read_operations_counter << "," << write_operations_counter << "," << merge_operations_counter << "," <<
            read_operations_counter + write_operations_counter << "," << expected_drive_operations << endl;
    }

    results_file.close();
}
