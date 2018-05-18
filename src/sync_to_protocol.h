#include "timestamp.h"

typedef tuple<ColumnValues, ColumnValues, size_t, size_t> KeyRangeWithRowCount;

struct HashResult {
	HashResult(const Table &table, ColumnValues prev_key, ColumnValues last_key, size_t estimated_rows_in_range, size_t our_row_count, size_t our_size, string our_hash, ColumnValues our_last_key):
		table(table), prev_key(prev_key), last_key(last_key), estimated_rows_in_range(estimated_rows_in_range), our_row_count(our_row_count), our_size(our_size), our_hash(our_hash), our_last_key(our_last_key) {}

	const Table &table;
	ColumnValues prev_key;
	ColumnValues last_key;
	size_t estimated_rows_in_range;

	size_t our_row_count;
	size_t our_size;
	string our_hash;
	ColumnValues our_last_key;
};

template <class Worker, class DatabaseClient>
struct SyncToProtocol {
	SyncToProtocol(Worker &worker):
		worker(worker),
		read_client(worker.read_client),
		sync_queue(worker.sync_queue),
		input(worker.input),
		output(worker.output),
		hash_algorithm(worker.configured_hash_algorithm),
		target_minimum_block_size(1),
		target_maximum_block_size(DEFAULT_MAXIMUM_BLOCK_SIZE) {
	}

	void negotiate_target_minimum_block_size() {
		send_command(output, Commands::TARGET_BLOCK_SIZE, DEFAULT_MINIMUM_BLOCK_SIZE);

		// the real app always accepts the block size we request, but the test suite uses smaller block sizes to make it easier to set up different scenarios
		read_expected_command(input, Commands::TARGET_BLOCK_SIZE, target_minimum_block_size);
	}

	void negotiate_hash_algorithm() {
		send_command(output, Commands::HASH_ALGORITHM, hash_algorithm);
		read_expected_command(input, Commands::HASH_ALGORITHM, hash_algorithm);
	}

	void sync_tables() {
		negotiate_target_minimum_block_size();
		negotiate_hash_algorithm();

		while (true) {
			// grab the next table to work on from the queue (blocking if it's empty)
			unique_ptr<TableWriteList<DatabaseClient>> table_write_list = sync_queue.pop_table_to_process();

			// quit if there's no more tables to process
			if (!table_write_list) break;

			// synchronize that table (unfortunately we can't share this job with other workers because next-key
			// locking is used for unique key indexes to enforce the uniqueness constraint, so we can't share
			// write traffic to the database across connections, which makes it somewhat futile to try and farm the
			// read work out since that needs to see changes made to satisfy unique indexes earlier in the table)
			sync_table(move(table_write_list));

			// TODO: how do we know the table is completed?
			//sync_queue.table_completed(table_write_list);
		}
	}

	void sync_table(unique_ptr<TableWriteList<DatabaseClient>> table_write_list) {
		const Table &table(table_write_list->table);

		size_t hash_commands = 0;
		size_t rows_commands = 0;
		time_t started = time(nullptr);

		if (worker.verbose) {
			unique_lock<mutex> lock(sync_queue.mutex);
			cout << fixed << setw(5);
			if (worker.verbose > 1) cout << timestamp() << ' ';
			cout << "starting " << table.name << endl << flush;
		}

		if (worker.verbose > 1) cout << timestamp() << " <- range " << table.name << endl;
		send_command(output, Commands::RANGE, table.name);
		table_write_list = handle_range_response(move(table_write_list));

		/*size_t outstanding_commands = 0, outstanding_rows_commands = 0, max_outstanding_commands = 2;*/

		while (true) {
			sync_queue.check_aborted(); // check each iteration, rather than wait until the end of the current table

			if (worker.progress) {
				cout << "." << flush; // simple progress meter
			}

			if (/*outstanding_commands < max_outstanding_commands && */table_write_list) {
				ColumnValues prev_key, last_key;

				tie(prev_key, last_key) = table_write_list->ranges_to_retrieve.front();
				table_write_list->ranges_to_retrieve.pop_front();

				if (worker.verbose > 1) cout << timestamp() << " <- rows " << table.name << ' ' << values_list(read_client, table, prev_key) << ' ' << values_list(read_client, table, last_key) << endl;
				send_command(output, Commands::ROWS, table.name, prev_key, last_key);
				rows_commands++;
				/*outstanding_commands++;
				outstanding_rows_commands++;*/

				handle_rows_response(table_write_list);

				if (table_write_list->ranges_to_retrieve.empty()) {
					table_write_list = sync_queue.completed_table_write_list(move(table_write_list));
				}

			} else if (/*outstanding_commands < max_outstanding_commands && */!__todo_ranges_to_check.empty()) {
				ColumnValues prev_key, last_key;
				size_t estimated_rows_in_range, rows_to_hash;

				tie(prev_key, last_key, estimated_rows_in_range, rows_to_hash) = __todo_ranges_to_check.front();
				__todo_ranges_to_check.pop_front();
				if (rows_to_hash == 0) throw logic_error("Can't hash 0 rows");

				// tell the other end to hash this range
				if (worker.verbose > 1) cout << timestamp() << " <- hash " << table.name << ' ' << values_list(read_client, table, prev_key) << ' ' << values_list(read_client, table, last_key) << ' ' << rows_to_hash << endl;
				send_command(output, Commands::HASH, table.name, prev_key, last_key, rows_to_hash);
				hash_commands++;
				/*outstanding_commands++;*/

				// while that end is working, do the same at our end
				RowHasherAndLastKey hasher(hash_algorithm, table.primary_key_columns);
				read_client.retrieve_rows(hasher, table, prev_key, last_key, rows_to_hash);
				ranges_hashed.push_back(HashResult(table, prev_key, last_key, estimated_rows_in_range, hasher.row_count, hasher.size, hasher.finish().to_string(), hasher.last_key));

				table_write_list = handle_hash_response(table, move(table_write_list));
/*
			} else if (outstanding_commands > 0) {
				verb_t verb;
				input >> verb;

				switch (verb) {
					case Commands::HASH:
						handle_hash_response(table_write_list);
						break;

					case Commands::ROWS:
						handle_rows_response(table_write_list);
						outstanding_rows_commands--;
						break;

					default:
						throw command_error("Unexpected command " + to_string(verb));
				}
				outstanding_commands--;
*/
			} else {
				// nothing left to do on this table
				break;
			}
		}

		// TODO: all this end-of-table code need to move elsewhere now?

		// make sure all pending updates have been applied
		if (table_write_list) {
			table_write_list->row_replacer.apply(worker.commit_level >= CommitLevel::often, [&] { if (worker.progress) { cout << "." << flush; } });
		}

		// reset sequences on those databases that don't automatically bump the high-water mark for inserts
		ResetTableSequences<DatabaseClient>::execute(*table_write_list->write_client, table);

		if (worker.verbose) {
			time_t now = time(nullptr);
			unique_lock<mutex> lock(sync_queue.mutex);
			if (worker.verbose > 1) cout << timestamp() << ' ';
			// TODO: how do we get the row_replacer?
//			cout << "finished " << table.name << " in " << (now - started) << "s using " << hash_commands << " hash commands and " << rows_commands << " rows commands changing " << row_replacer.rows_changed << " rows" << endl << flush;
		}

		if (worker.commit_level >= CommitLevel::tables) {
			table_write_list->write_client->commit_transaction();
			table_write_list->write_client->start_write_transaction();
		}
	}

	unique_ptr<TableWriteList<DatabaseClient>> handle_range_response(unique_ptr<TableWriteList<DatabaseClient>> table_write_list) {
		if (!table_write_list) throw runtime_error("should always own the table_write_list when starting a table");
		if (input.next<verb_t>() != Commands::RANGE) throw runtime_error("expected RANGE response");

		// we're going to make a few initial special writes below; after that everything is done by
		// the row replacer.  we need to write to the same connection as it will for this table, so
		// the row locks don't fight.
		DatabaseClient &write_client(table_write_list->row_replacer.client);

		string _table_name;
		ColumnValues their_first_key, their_last_key;
		read_all_arguments(input, _table_name, their_first_key, their_last_key);
		if (worker.verbose > 1) cout << timestamp() << " -> range " << table_write_list->table.name << ' ' << values_list(write_client, table_write_list->table, their_first_key) << ' ' << values_list(write_client, table_write_list->table, their_last_key) << endl;

		if (their_first_key.empty()) {
			write_client.execute("DELETE FROM " + table_write_list->table.name);
			return sync_queue.completed_table_write_list(move(table_write_list));
		}

		// we immediately know that we need to clear everything < their_first_key or > their_last_key; do that now
		string key_columns(columns_list(write_client, table_write_list->table.columns, table_write_list->table.primary_key_columns));
		string delete_from("DELETE FROM " + table_write_list->table.name + " WHERE " + key_columns);
		write_client.execute(delete_from + " < " + values_list(write_client, table_write_list->table, their_first_key));
		write_client.execute(delete_from + " > " + values_list(write_client, table_write_list->table, their_last_key));

		// having done that, find our last key, which must now be no greater than their_last_key
		ColumnValues our_last_key(write_client.last_key(table_write_list->table));

		// we immediately know that we need to retrieve any new rows > our_last_key, unless their last key is the same;
		// queue this up
		if (our_last_key != their_last_key) {
			table_write_list->ranges_to_retrieve.push_back(make_tuple(our_last_key, their_last_key));
		} else {
			table_write_list = sync_queue.completed_table_write_list(move(table_write_list));
		}

		// queue up a sync of everything up to our_last_key; the way we have defined key ranges to work, we have no way
		// to express start-inclusive ranges to the sync methods, so we queue a sync from the start (empty key value)
		// up to the last key, but this results in no actual inefficiency because they'd see the same rows anyway.
		if (!our_last_key.empty()) {
			__todo_ranges_to_check.push_back(make_tuple(ColumnValues(), our_last_key, UNKNOWN_ROW_COUNT, 1 /* start with 1 row and build up */));
		}

		return table_write_list;
	}

	void handle_rows_response(unique_ptr<TableWriteList<DatabaseClient>> &table_write_list) {
		const Table &table(table_write_list->table);

		// we're being sent a range of rows; apply them to our end.  we do this in-context to
		// provide flow control - if we buffered and used a separate apply thread, we would
		// bloat up if this end couldn't write to disk as quickly as the other end sent data.
		string table_name;
		ColumnValues prev_key, last_key;
		read_array(input, table_name, prev_key, last_key); // the first array gives the range arguments, which is followed by one array for each row
		if (worker.verbose > 1) cout << timestamp() << " -> rows " << table.name << ' ' << values_list(read_client, table, prev_key) << ' ' << values_list(read_client, table, last_key) << endl;

		RowRangeApplier<DatabaseClient>(table_write_list->row_replacer, worker.commit_level >= CommitLevel::often, table, prev_key, last_key, [&] { if (worker.progress) { cout << "." << flush; } }).stream_from_input(input);
	}

	HashResult find_hash_result_for(const Table &table, const ColumnValues &prev_key, const ColumnValues &last_key) {
		for (auto it = ranges_hashed.begin(); it != ranges_hashed.end(); ++it) {
			if (it->table.name == table.name && it->prev_key == prev_key && it->last_key == last_key) {
				HashResult result(*it);
				ranges_hashed.erase(it);
				return result;
			}
		}
		throw command_error("Haven't issued a hash command for " + table.name + " " + values_list(read_client, table, prev_key) + " " + values_list(read_client, table, last_key));
	}

	unique_ptr<TableWriteList<DatabaseClient>> handle_hash_response(const Table &table, unique_ptr<TableWriteList<DatabaseClient>> table_write_list) {
		size_t rows_to_hash, their_row_count;
		string their_hash;
		string table_name;
		ColumnValues prev_key, last_key;
		read_all_arguments(input, table_name, prev_key, last_key, rows_to_hash, their_row_count, their_hash);
		
		HashResult hash_result(find_hash_result_for(table, prev_key, last_key));

		bool match = (hash_result.our_hash == their_hash && hash_result.our_row_count == their_row_count);
		if (worker.verbose > 1) cout << timestamp() << " -> hash " << table.name << ' ' << values_list(read_client, table, prev_key) << ' ' << values_list(read_client, table, last_key) << ' ' << their_row_count << (match ? " matches" : " doesn't match") << endl;

		if (hash_result.our_row_count == rows_to_hash && hash_result.our_last_key != last_key) {
			// whether or not we found an error in the range we just did, we don't know whether
			// there is an error in the remaining part of the original range (which could be simply
			// the rest of the table); queue it to be scanned
			if (hash_result.estimated_rows_in_range == UNKNOWN_ROW_COUNT) {
				// we're scanning forward, do that last
				size_t rows_to_hash = rows_to_scan_forward_next(rows_to_hash, match, hash_result.our_row_count, hash_result.our_size);
				__todo_ranges_to_check.push_back(make_tuple(hash_result.our_last_key, last_key, UNKNOWN_ROW_COUNT, rows_to_hash));
			} else {
				// we're hunting errors, do that first.  if the part just checked matched, then the
				// error must be in the remaining part, so consider subdividing it; if it didn't match,
				// then we don't know if the remaining part has error or not, so don't subdivide it yet.
				size_t rows_remaining = hash_result.estimated_rows_in_range > hash_result.our_row_count ? hash_result.estimated_rows_in_range - hash_result.our_row_count : 1; // conditional to protect against underflow
				size_t rows_to_hash = match && rows_remaining > 1 && hash_result.our_size > target_minimum_block_size ? rows_remaining/2 : rows_remaining;
				__todo_ranges_to_check.push_front(make_tuple(hash_result.our_last_key, last_key, rows_remaining, rows_to_hash));
			}
		}

		if (!match) {
			// the part that we checked has an error; decide whether it's large enough to subdivide
			if (hash_result.our_row_count > 1 && hash_result.our_size > target_minimum_block_size) {
				// yup, queue it up for another iteration of hashing, subdividing the range
				__todo_ranges_to_check.push_front(make_tuple(prev_key, hash_result.our_last_key, hash_result.our_row_count, hash_result.our_row_count/2));
			} else {
				// not worth subdividing the range any further, queue it to be retrieved
				if (table_write_list) {
					table_write_list->ranges_to_retrieve.push_back(make_tuple(prev_key, hash_result.our_last_key));
				} else {
					table_write_list = sync_queue.push_range_to_retrieve(table, make_tuple(prev_key, hash_result.our_last_key));
				}
			}
		}

		return table_write_list;
	}

	inline size_t rows_to_scan_forward_next(size_t rows_scanned, bool match, size_t our_row_count, size_t our_size) {
		if (match) {
			// on the next iteration, scan more rows per iteration, to reduce the impact of latency between the ends -
			// up to a point, after which the cost of re-work when we finally run into a mismatch outweights the
			// benefit of the latency savings
			if (our_size <= target_maximum_block_size/2) {
				return max<size_t>(our_row_count*2, 1);
			} else {
				return max<size_t>(our_row_count*target_maximum_block_size/our_size, 1);
			}
		} else {
			// on the next iteration, scan fewer rows per iteration, to reduce the cost of re-work (down to a point)
			if (our_size >= target_minimum_block_size*2) {
				return max<size_t>(our_row_count/2, 1);
			} else {
				return max<size_t>(our_row_count*target_minimum_block_size/our_size, 1);
			}
		}
	}

	Worker &worker;
	DatabaseClient &read_client;
	SyncQueue<DatabaseClient> &sync_queue;
	Unpacker<FDReadStream> &input;
	Packer<FDWriteStream> &output;
	HashAlgorithm hash_algorithm;
	size_t target_minimum_block_size;
	size_t target_maximum_block_size;

	// contains the ranges that we have hashed, and asked the other end to hash.  this is a
	// per-worker thing because the specific peer that we sent the request to will respond.
	list<HashResult> ranges_hashed;

	// TODO: move to SyncQueue and share
	list<KeyRangeWithRowCount> __todo_ranges_to_check;
};
