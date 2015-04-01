// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_INTEGRATION_TESTS_TEST_WORKLOAD_H
#define KUDU_INTEGRATION_TESTS_TEST_WORKLOAD_H

#include <vector>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/atomic.h"
#include "kudu/util/countdown_latch.h"

namespace kudu {

class ExternalMiniCluster;
class Thread;

// Utility class for generating a workload against a test cluster.
//
// The actual data inserted is random, and thus can't be verified for
// integrity. However, this is still useful in conjunction with ClusterVerifier
// to verify that replicas do not diverge.
class TestWorkload {
 public:
  explicit TestWorkload(ExternalMiniCluster* cluster);
  ~TestWorkload();

  void set_num_write_threads(int n) {
    num_write_threads_ = n;
  }

  void set_write_batch_size(int s) {
    write_batch_size_ = s;
  }

  void set_write_timeout_millis(int t) {
    write_timeout_millis_ = t;
  }

  void set_timeout_allowed(bool allowed) {
    timeout_allowed_ = allowed;
  }

  // Creates the internal client, and the table which will be used
  // for writing.
  void Setup();

  // Start the write workload.
  void Start();

  // Stop the writers and wait for them to exit.
  void StopAndJoin();

  // Return the number of rows inserted so far. This may be called either
  // during or after the write workload.
  int64_t rows_inserted() const {
    return rows_inserted_.Load();
  }

 private:
  void WriteThread();

  ExternalMiniCluster* cluster_;
  std::tr1::shared_ptr<client::KuduClient> client_;

  int num_write_threads_;
  int write_batch_size_;
  int write_timeout_millis_;
  bool timeout_allowed_;

  int num_replicas_;

  CountDownLatch start_latch_;
  AtomicBool should_run_;
  AtomicInt<int64_t> rows_inserted_;

  std::vector<scoped_refptr<Thread> > threads_;

  DISALLOW_COPY_AND_ASSIGN(TestWorkload);
};

} // namespace kudu
#endif /* KUDU_INTEGRATION_TESTS_TEST_WORKLOAD_H */