#include "stdio.h"
#include "stdlib.h"


void modify_prev (int * a) {
	a[-1] = -1;
}

int main () {
	int *a = (int *)malloc(2 * sizeof(int));
	a[0] = 1;
	a[1] = 2;
	modify_prev(&a[1]);
	printf("%d\n", (a+1)[-1]);
	return 0;
}
