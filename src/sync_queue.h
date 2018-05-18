#ifndef SYNC_QUEUE_H
#define SYNC_QUEUE_H

#include <queue>
#include <list>
#include <set>
#include <unordered_map>
#include <memory>

#include "abortable_barrier.h"
#include "schema.h"
#include "row_replacer.h"

using namespace std;

typedef tuple<ColumnValues, ColumnValues> KeyRange;

typedef tuple<const Table&, ColumnValues, ColumnValues, size_t, size_t> TableKeyRangeWithRowCount;
const size_t UNKNOWN_ROW_COUNT = numeric_limits<size_t>::max();

template <typename DatabaseClient>
struct TableWriteList {
	TableWriteList(const Table &table, unique_ptr<DatabaseClient> write_client):
		table(table),
		write_client(move(write_client)),
		row_replacer(*write_client.get(), table) {
	}

	const Table &table;
	unique_ptr<DatabaseClient> write_client;
	RowReplacer<DatabaseClient> row_replacer;
	deque<KeyRange> ranges_to_retrieve;

private:
	// non-copyable
	TableWriteList(const TableWriteList &);
	TableWriteList &operator=(const TableWriteList &);
};

template <typename DatabaseClient>
struct TableBeingProcessed {
	TableBeingProcessed(const Table &table, unique_ptr<DatabaseClient> write_client):
		table(table),
		table_write_list(make_unique<TableWriteList<DatabaseClient>>(table, move(write_client))) {
	}

	const Table &table;
	unique_ptr<TableWriteList<DatabaseClient>> table_write_list;
	deque<KeyRange> ranges_to_retrieve_not_on_table_write_list_yet;

private:
	// non-copyable
	TableBeingProcessed(const TableBeingProcessed &);
	TableBeingProcessed &operator=(const TableBeingProcessed &);
};

template <typename DatabaseClient>
struct SyncQueue: public AbortableBarrier {
	SyncQueue(size_t workers): AbortableBarrier(workers) {}

	void push_database_writer(unique_ptr<DatabaseClient> write_client) {
		unique_lock<std::mutex> lock(mutex);

		available_database_writers.push_back(move(write_client));
	}

	void enqueue_tables_to_process(const Tables &tables) {
		unique_lock<std::mutex> lock(mutex);

		for (const Table &from_table : tables) {
			tables_to_process.push_back(&from_table);
		}
	}

	unique_ptr<TableWriteList<DatabaseClient>> pop_table_to_process() {
		unique_lock<std::mutex> lock(mutex);
		if (aborted) throw aborted_error();
		if (tables_to_process.empty()) return nullptr;

		if (available_database_writers.empty()) throw runtime_error("needed more writers than in pool!");
		unique_ptr<DatabaseClient> write_client(move(available_database_writers.front()));
		available_database_writers.pop_front();

		const Table *table = tables_to_process.front();
		tables_to_process.pop_front();

		shared_ptr<TableBeingProcessed<DatabaseClient>> table_being_processed = make_shared<TableBeingProcessed<DatabaseClient>>(*table, move(write_client));
		tables_being_processed[table] = table_being_processed;
		return move(table_being_processed->table_write_list);
	}

	unique_ptr<TableWriteList<DatabaseClient>> push_range_to_retrieve(const Table &table, const KeyRange &key_range) {
		unique_lock<std::mutex> lock(mutex);

		shared_ptr<TableBeingProcessed<DatabaseClient>> table_being_processed = tables_being_processed[&table];
		if (!table_being_processed) throw runtime_error("tried to push range for table not being processed");

		if (table_being_processed->table_write_list) {
			// no-one else is writing to this table right now, so put the key range onto the list and hand the table_write_list out to the calling worker
			table_being_processed->table_write_list->ranges_to_retrieve.push_back(key_range);
			return move(table_being_processed->table_write_list);
		} else {
			// the table_write_list has been handed out to another worker, so queue this up to be given out later
			table_being_processed->ranges_to_retrieve_not_on_table_write_list_yet.push_back(key_range);
			return nullptr;
		}
	}

	unique_ptr<TableWriteList<DatabaseClient>> completed_table_write_list(unique_ptr<TableWriteList<DatabaseClient>> table_write_list) {
		unique_lock<std::mutex> lock(mutex);

		shared_ptr<TableBeingProcessed<DatabaseClient>> table_being_processed = tables_being_processed[&table_write_list->table];
		if (!table_being_processed) throw runtime_error("tried to complete list for table not being processed");

		if (table_being_processed->ranges_to_retrieve_not_on_table_write_list_yet.empty()) {
			// no more work to do, check the table_write_list back in for another worker to use when next required
			table_being_processed->table_write_list = move(table_write_list);
			return nullptr;
		} else {
			swap(table_being_processed->ranges_to_retrieve_not_on_table_write_list_yet, table_write_list->ranges_to_retrieve);
			return table_write_list;
		}
	}
/*
	void table_completed(shared_ptr<TableWriteList<DatabaseClient>> table_write_list) {
		unique_lock<std::mutex> lock(mutex);
		available_database_writers.push_back(move(table_write_list->write_client));
		table_write_lists.erase(table_write_list->table);
	}
*/	
	string snapshot;

private:
	list<const Table*> tables_to_process;
	list<unique_ptr<DatabaseClient>> available_database_writers;
	unordered_map<const Table*, shared_ptr<TableBeingProcessed<DatabaseClient>>> tables_being_processed;
	list<TableKeyRangeWithRowCount> table_ranges_to_check;
};

#endif
