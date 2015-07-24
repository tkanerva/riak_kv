#pragma D option quiet
#pragma D option dynvarsize=128m

uint64_t encoding_acc;
uint64_t encoding_start;
uint64_t outer_encoding_acc;
uint64_t outer_encoding_start;
/* First time we notice the PB process being scheduled. */
uint64_t pb_first_start;
uint64_t pb_stop;
/* Set when script has identified the PB server Erlang process */
int pb_proc_set;
/* Pid of the PB server Erlang process */
string pb_proc;
string pb_proc_mfa;
/* Timestamp of the last time the process was descheduled */
uint64_t pb_stop;

/* Mark first process that calls dyntrace:p(1,...) as the PB server we
   are looking at, remember its pid. */
erlang$target:::user_trace-i4s4
/ (arg2 == 1 || arg2 == 3) && !pb_proc_set /
{
    pb_proc = copyinstr(arg0);
    pb_proc_set = 1;
    printf("PB process id is %s\n", pb_proc);
}

/* Enter outer encoding section */
erlang$target:::user_trace-i4s4
/ arg2 == 3 /
{
    outer_encoding_start = vtimestamp;
}

/* Leave outer encoding section */
erlang$target:::user_trace-i4s4
/ arg2 == 4 && outer_encoding_start /
{
    this->elapsed = vtimestamp - outer_encoding_start;
    @outer_encoding_time = sum(this->elapsed);
    outer_encoding_start = 0;
}

/* Enter encoding section */
erlang$target:::user_trace-i4s4
/ arg2 == 1 /
{
    encoding_start = vtimestamp;
}

/* Leave encoding section */
erlang$target:::user_trace-i4s4
/ arg2 == 2 && encoding_start /
{
    this->elapsed = vtimestamp - encoding_start;
    @encoding_time = sum(this->elapsed);
    @encoding_time_total = sum(this->elapsed);
    @encoding_time_avg = avg(this->elapsed);
    @encoding_time_min = min(this->elapsed);
    @encoding_time_max = max(this->elapsed);
    @encoding_count = count();
    encoding_start = 0;
}

/* First time we start measuring PB process time. */
erlang$target:::process-scheduled
/ copyinstr(arg0) == pb_proc && !pb_first_start /
{
    pb_first_start = timestamp;
}

/* PB server process scheduled after blocking on sending bytes
 * down the socket.
 */
erlang$target:::process-scheduled
/ copyinstr(arg0) == pb_proc
  && copyinstr(arg1) == "prim_inet:send/3"
  && pb_stop /
{
    this->elapsed = timestamp - pb_stop;
    @send_time = sum(this->elapsed);
    @total_send_time = sum(this->elapsed);
}

/* Re-scheduled during outer encoding. */
erlang$target:::process-scheduled
/ copyinstr(arg0) == pb_proc && outer_encoding_acc /
{
    /* Fake starting time to include previous time */
    outer_encoding_start = vtimestamp - outer_encoding_acc;
    outer_encoding_acc = 0;
}
/* Re-scheduled during encoding. */
erlang$target:::process-scheduled
/ copyinstr(arg0) == pb_proc && encoding_acc /
{
    /* Fake starting time to include previous time */
    encoding_start = vtimestamp - encoding_acc;
    encoding_acc = 0;
}

erlang$target:::process-scheduled
/ copyinstr(arg0) == pb_proc /
{
    pb_stop = 0;
    self->pb_start = timestamp;
    self->pb_vstart = vtimestamp;
    @pb_scheduled = count();
    /*
    this->mfa = copyinstr(arg1);
    @scheduled_mfa[this->mfa] = count();
    pb_proc_mfa = this->mfa;
    */
}

/* De-scheduled during encoding. Remember encoding time so far. */
erlang$target:::process-unscheduled
/ self->pb_start && encoding_start /
{
    encoding_acc = vtimestamp - encoding_start;
    encoding_start = 0;
}

/* De-scheduled during outer encoding. Remember encoding time so far. */
erlang$target:::process-unscheduled
/ self->pb_start && outer_encoding_start /
{
    outer_encoding_acc = vtimestamp - outer_encoding_start;
    outer_encoding_start = 0;
}

erlang$target:::process-unscheduled
/ self->pb_start /
{
    this->elapsed = timestamp - self->pb_start;
    this->velapsed = vtimestamp - self->pb_vstart;
    @proc_time = sum(this->elapsed);
    @proc_vtime = sum(this->velapsed);
    @proc_time_total = sum(this->elapsed);
    @proc_vtime_total = sum(this->velapsed);
    self->pb_start = 0;
    pb_stop = timestamp;
}

/* ============================= Measure garbage collection ================*/
/* Store starting time in thread for GCs */
erlang$target:::gc_minor-start
/ copyinstr(arg0) == pb_proc /
{
    self->gc_minor_start = vtimestamp;
}

erlang$target:::gc_major-start
/ copyinstr(arg0) == pb_proc /
{
    self->gc_major_start = vtimestamp;
}

/* GC time during encoding */
erlang$target:::gc_major-end
/ self->gc_major_start && encoding_start /
{
    this->elapsed = vtimestamp - self->gc_major_start;
    @gc_major_encoding_time = sum(this->elapsed);
    @gc_major_encoding_time_total = sum(this->elapsed);
}

erlang$target:::gc_minor-end
/ self->gc_minor_start  && encoding_start /
{
    this->elapsed = vtimestamp - self->gc_minor_start;
    @gc_minor_encoding_time = sum(this->elapsed);
    @gc_minor_encoding_time_total = sum(this->elapsed);
}

/* Total GC time on behalf of PB process. */
erlang$target:::gc_minor-end
/ self->gc_minor_start /
{
    this->elapsed = vtimestamp - self->gc_minor_start;
    @gc_minor_time = sum(this->elapsed);
    @gc_minor_time_total = sum(this->elapsed);
    @gc_minor_count = count();
    self->gc_minor_start = 0;
}

erlang$target:::gc_major-end
/ self->gc_major_start /
{
    this->elapsed = vtimestamp - self->gc_major_start;
    @gc_major_time = sum(this->elapsed);
    @gc_major_time_avg = avg(this->elapsed);
    @gc_major_time_total = sum(this->elapsed);
    @gc_major_count = count();
    self->gc_major_start = 0;
}

profile:::tick-1s
{
    printf("===================================================\n");
    printa("PB proc scheduled %@u times\n", @pb_scheduled);
    clear(@pb_scheduled);

    /*
    trunc(@scheduled_mfa, 20);
    printf("PB proc top de-scheduled MFAs: \n");
    printa(@scheduled_mfa);
    trunc(@scheduled_mfa);
    */

    normalize(@proc_time, 1000000);
    printa("PB proc time : %@ums\n", @proc_time);
    clear(@proc_time);
    
    normalize(@proc_vtime, 1000000);
    printa("PB proc vtime : %@ums\n", @proc_vtime);
    clear(@proc_vtime);
    
    normalize(@send_time, 1000000);
    printa("Send time : %@ums\n", @send_time);
    clear(@send_time);

    normalize(@gc_major_time, 1000000);
    normalize(@gc_major_time_avg, 1000000);
    printa("Major GC count : %@u\n", @gc_major_count);
    printa("Major GC time : %@ums\n", @gc_major_time);
    printa("Major GC time avg. : %@ums\n", @gc_major_time_avg);
    clear(@gc_major_time);
    clear(@gc_major_time_avg);
    clear(@gc_major_count);

    normalize(@gc_minor_time, 1000000);
    printa("Minor GC count : %@u\n", @gc_minor_count);
    printa("Minor GC time : %@ums\n", @gc_minor_time);
    clear(@gc_minor_time);
    clear(@gc_minor_count);

    normalize(@gc_major_encoding_time, 1000000);
    printa("Major GC during encoding : %@ums\n", @gc_major_encoding_time);
    clear(@gc_major_encoding_time);

    normalize(@gc_minor_encoding_time, 1000000);
    printa("Minor GC during encoding : %@ums\n", @gc_minor_encoding_time);
    clear(@gc_minor_encoding_time);

    printa("Objects encoded : %@u\n", @encoding_count);
    clear(@encoding_count);

    normalize(@encoding_time, 1000000);
    normalize(@outer_encoding_time, 1000000);
    normalize(@encoding_time_avg, 1000);
    normalize(@encoding_time_min, 1000);
    normalize(@encoding_time_max, 1000);
    printa("Outer Encoding Time : %@10ums\n", @outer_encoding_time);
    printa("Encoding Time : %@10ums\n", @encoding_time);
    printa("Encoding Avg time : %@10uus\n", @encoding_time_avg);
    printa("Encoding Min time : %@10uus\n", @encoding_time_min);
    printa("Encoding Max time : %@10uus\n", @encoding_time_max);
    /*
    printf("Encoding Time histogram:\n");
    printa(@encoding_time_hist);
    */
    clear(@outer_encoding_time);
    clear(@encoding_time);
    clear(@encoding_time_avg);
    clear(@encoding_time_min);
    clear(@encoding_time_max);
    /* trunc(@encoding_time_hist); */

    normalize(@total_send_time, 1000000);
    normalize(@proc_time_total, 1000000);
    normalize(@proc_vtime_total, 1000000);
    normalize(@encoding_time_total, 1000000);
    normalize(@gc_major_time_total, 1000000);
    normalize(@gc_minor_time_total, 1000000);
    normalize(@gc_major_encoding_time_total, 1000000);
    normalize(@gc_minor_encoding_time_total, 1000000);
    self->total_time = pb_first_start? timestamp - pb_first_start : 0;
    printf("Total time : %ums\n", self->total_time / 1000000);
    printa("Total PB proc time : %@ums\n", @proc_time_total);
    printa("Total PB proc vtime : %@ums\n", @proc_vtime_total);
    printa("Total PB proc send time : %@ums\n", @total_send_time);
    printa("Total encoding time: %@10ums\n", @encoding_time_total);
    printa("Total GC major encoding time: %@ums\n", @gc_major_encoding_time_total);
    printa("Total GC minor encoding time: %@ums\n", @gc_minor_encoding_time_total);
    printa("Total GC major time: %@ums\n", @gc_major_time_total);
    printa("Total GC minor time: %@ums\n", @gc_minor_time_total);
    

    /*
    trunc(@stacks, 10);
    printa(@stacks);
    trunc(@stacks);
    */
}
