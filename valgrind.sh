valgrind --leak-check=full \
         --show-leak-kinds=all \
         --track-origins=yes \
         --leak-resolution=high \
         --log-file=valgrind_analysis.log \
         ./build/c_kafka_engine
