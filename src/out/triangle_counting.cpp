#include <fcntl.h>
#include <errno.h>
#include <err.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <stdio.h>
#include <stdint.h>
#include <unistd.h>
#include <time.h>

#include "Encoding.hpp"
#include "Trie.hpp"
#include "TrieBuilder.hpp"
#include "TrieIterator.hpp"
#include "utils/thread_pool.hpp"
#include "utils/timer.hpp"

#ifndef MAP_FILE
#define MAP_FILE MAP_SHARED
#endif
int64_t fsize(int fd) {
  struct stat stat;
  int64_t res = fstat(fd,&stat);
  return stat.st_size;
}
int printll(char* s) {
  while (*s != '\n' && *s != ',' && *s != '\t') {
    putchar(*s++);
  }
  return 0;
}
long hash(char *str0, int len)
{
  unsigned char* str = (unsigned char*)str0;
  unsigned long hash = 5381;
  int c;
  while ((c = *str++) && len--)
  hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
  return hash;
}
void Snippet(char*);
int main(int argc, char *argv[])
{
  if (argc != 2) {
    printf("usage: query <filename>\n");
    return 0;
  }
  Snippet(argv[1]);
  return 0;
}
/*****************************************
Emitting C Generated Code
*******************************************/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
void Snippet(char*  x0) {
  printf("%s\n","0,1,2");
  std::unordered_map<std::string, void *>* input_tries = new std::unordered_map<std::string, void *>();
  thread_pool::initializeThreadPool();
  Trie<void *, ParMemoryBuffer> *Trie_Edge_0_1 = NULL;
  Trie_Edge_0_1 = Trie<void *, ParMemoryBuffer>::load("/home/wu636/cs525_project/test/8_threads/relations/Edge/Edge_0_1");
  Encoding<uint32_t> *Encoding_uint32_t = Encoding<uint32_t>::from_binary("/home/wu636/cs525_project/test/8_threads/encodings/uint32_t/");
  par::reducer<size_t> num_rows_reducer(0, [](size_t a, size_t b) { return a + b; });
  auto query_timer = timer::start_clock();
  Trie<void *, ParMemoryBuffer> *Trie_TriangleList_0_1_2 = new Trie<void *, ParMemoryBuffer>("/home/wu636/cs525_project/test/2_threads/relations/TriangleList", 3, false);
  {
    ParTrieBuilder<void *, ParMemoryBuffer> Builders(Trie_TriangleList_0_1_2,3);
    Builders.trie->encodings.push_back((void *)Encoding_uint32_t);
    ParTrieIterator<void *, ParMemoryBuffer> Iterators_Edge_0_1(Trie_Edge_0_1);
    Builders.trie->encodings.push_back((void *)Encoding_uint32_t);
    ParTrieIterator<void *, ParMemoryBuffer> Iterators_Edge_0_2(Trie_Edge_0_1);
    Builders.trie->encodings.push_back((void *)Encoding_uint32_t);
    ParTrieIterator<void *, ParMemoryBuffer> Iterators_Edge_1_2(Trie_Edge_0_1);
    const size_t count_a = Builders.build_set(Iterators_Edge_0_1.head, Iterators_Edge_0_2.head);
    Builders.allocate_next();
    Builders.par_foreach_builder([&](const size_t tid,const uint32_t a_i,const uint32_t a_d){
      TrieBuilder<void*,ParMemoryBuffer>*Builder=Builders.builders.at(tid);
      TrieIterator<void*,ParMemoryBuffer>*Iterator_Edge_0_1=Iterators_Edge_0_1.iterators.at(tid);
      TrieIterator<void*,ParMemoryBuffer>*Iterator_Edge_1_2=Iterators_Edge_1_2.iterators.at(tid);
      TrieIterator<void*,ParMemoryBuffer>*Iterator_Edge_0_2=Iterators_Edge_0_2.iterators.at(tid);
      Iterator_Edge_0_1->get_next_block(0,a_d);
      Iterator_Edge_0_2->get_next_block(0,a_d);
      const size_t count_b=Builder->build_set(tid,Iterator_Edge_0_1->get_block(1),Iterator_Edge_1_2->get_block(0));
      Builder->allocate_next(tid);
      Builder->foreach_builder([&](const uint32_t b_i,const uint32_t b_d){
        Iterator_Edge_1_2->get_next_block(0,b_d);
        const size_t count_c=Builder->build_set(tid,Iterator_Edge_1_2->get_block(1),Iterator_Edge_0_2->get_block(1));
        num_rows_reducer.update(tid,count_c);
        Builder->set_level(b_i,b_d);
      });
      Builder->set_level(a_i,a_d);
    });
    Builders.trie->num_rows = num_rows_reducer.evaluate(0);
    std::cout << "NUM ROWS: " << Builders.trie->num_rows << " ANNOTATION: " << Builders.trie->annotation << std::endl;
  }
  timer::stop_clock("QUERY TIME", query_timer);

  input_tries->insert(std::make_pair("TriangleList_0_1_2", Trie_TriangleList_0_1_2));
  thread_pool::deleteThreadPool();
  ;
}
/*****************************************
  End of C Generated Code
*******************************************/
 
