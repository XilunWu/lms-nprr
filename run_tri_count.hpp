#include "Encoding.hpp"
#include "Trie.hpp"
#include "TrieBuilder.hpp"
#include "TrieIterator.hpp"
#include "utils/thread_pool.hpp"
#include "utils/timer.hpp"

typedef std::unordered_map<std::string, void *> mymap;
typedef std::string string;

void run_0(mymap *input_tries) {
  thread_pool::initializeThreadPool();
  Trie<void *, ParMemoryBuffer> *Trie_Edge_0_1 = NULL;
  {
    auto start_time = timer::start_clock();
    Trie_Edge_0_1 = Trie<void *, ParMemoryBuffer>::load(
        "/home/wu636/Engines/EmptyHeaded/test/graph/databases/"
        "triangle_counting/relations/Edge/Edge_0_1");
    timer::stop_clock("LOADING Trie Edge_0_1", start_time);
  }

  auto e_loading_uint32_t = timer::start_clock();
  Encoding<uint32_t> *Encoding_uint32_t = Encoding<uint32_t>::from_binary(
      "/home/wu636/Engines/EmptyHeaded/test/graph/databases/triangle_counting/"
      "encodings/uint32_t/");
  (void)Encoding_uint32_t;
  timer::stop_clock("LOADING ENCODINGS uint32_t", e_loading_uint32_t);
  par::reducer<size_t> num_rows_reducer(
      0, [](size_t a, size_t b) { return a + b; });
  //
  // query plan
  //
  auto query_timer = timer::start_clock();
  Trie<void *, ParMemoryBuffer> *Trie_TriangleList_0_1_2 =
      new Trie<void *, ParMemoryBuffer>("/home/wu636/Engines/EmptyHeaded/test/"
                                        "graph/databases/triangle_counting/"
                                        "relations/TriangleList",
                                        3, false);
  {
    auto bag_timer = timer::start_clock();
    num_rows_reducer.clear();
    ParTrieBuilder<void *, ParMemoryBuffer> Builders(Trie_TriangleList_0_1_2,
                                                     3);
    Builders.trie->encodings.push_back((void *)Encoding_uint32_t);
    Builders.trie->encodings.push_back((void *)Encoding_uint32_t);
    Builders.trie->encodings.push_back((void *)Encoding_uint32_t);
    ParTrieIterator<void *, ParMemoryBuffer> Iterators_Edge_a_b(Trie_Edge_0_1);
    ParTrieIterator<void *, ParMemoryBuffer> Iterators_Edge_b_c(Trie_Edge_0_1);
    ParTrieIterator<void *, ParMemoryBuffer> Iterators_Edge_a_c(Trie_Edge_0_1);
    const size_t count_a =
        Builders.build_set(Iterators_Edge_a_b.head, Iterators_Edge_a_c.head);
    Builders.allocate_next();
    Builders.par_foreach_builder([&](const size_t tid, const uint32_t a_i,
                                     const uint32_t a_d) {
      TrieBuilder<void *, ParMemoryBuffer> *Builder = Builders.builders.at(tid);
      TrieIterator<void *, ParMemoryBuffer> *Iterator_Edge_a_b =
          Iterators_Edge_a_b.iterators.at(tid);
      TrieIterator<void *, ParMemoryBuffer> *Iterator_Edge_b_c =
          Iterators_Edge_b_c.iterators.at(tid);
      TrieIterator<void *, ParMemoryBuffer> *Iterator_Edge_a_c =
          Iterators_Edge_a_c.iterators.at(tid);
      Iterator_Edge_a_b->get_next_block(0, a_d);
      Iterator_Edge_a_c->get_next_block(0, a_d);
      const size_t count_b =
          Builder->build_set(tid, Iterator_Edge_a_b->get_block(1),
                             Iterator_Edge_b_c->get_block(0));
      Builder->allocate_next(tid);
      Builder->foreach_builder([&](const uint32_t b_i, const uint32_t b_d) {
        Iterator_Edge_b_c->get_next_block(0, b_d);
        const size_t count_c =
            Builder->build_set(tid, Iterator_Edge_b_c->get_block(1),
                               Iterator_Edge_a_c->get_block(1));
        num_rows_reducer.update(tid, count_c);
        Builder->set_level(b_i, b_d);
      });
      Builder->set_level(a_i, a_d);
    });
    Builders.trie->num_rows = num_rows_reducer.evaluate(0);
    std::cout << "NUM ROWS: " << Builders.trie->num_rows
              << " ANNOTATION: " << Builders.trie->annotation << std::endl;
    timer::stop_clock("BAG TriangleList TIME", bag_timer);
    Trie_TriangleList_0_1_2->memoryBuffers = Builders.trie->memoryBuffers;
    Trie_TriangleList_0_1_2->num_rows = Builders.trie->num_rows;
    Trie_TriangleList_0_1_2->encodings = Builders.trie->encodings;
  }
  input_tries->insert(
      std::make_pair("TriangleList_0_1_2", Trie_TriangleList_0_1_2));

  timer::stop_clock("QUERY TIME", query_timer);

  thread_pool::deleteThreadPool();
}
