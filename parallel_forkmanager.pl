use Data::Dumper;
use Parallel::ForkManager;
use feature 'say';

use utf8;
binmode(STDOUT, ':encoding(UTF-8)');
binmode(STDIN, ':encoding(UTF-8)');

die "Usage: perl forkie.pl <measurement file>\n" if ! @ARGV;
my $filename = $ARGV[0];
my $processes = 8;
my @child_pids = ();

open(F, "<$filename") or die "file does not exist\n";
my ($dev,$ino,$mode,$nlink,$uid,$gid,$rdev,$size, $atime,$mtime,$ctime,$blksize,$blocks) = stat F;
close(F);

print "Size of mapped file is $size\n";
my $i = $size / $processes; # Every process does this many bytes.

my $pm = Parallel::ForkManager->new($processes);

my $pid;
foreach my $p (0..$processes - 1) {
	my $start = int($i * $p);
	my $end = int($i * ($p + 1) - 1);
	$pid = $pm->start();
	if(!$pid) { # Child process
		%h;
		print "$$ Going from $start to $end\n";
		# We divide the file into equal byte size chunks and have each process start after the first occurence of a newline (as such you're sure that you start at the beginning of a line)
		# Two possibilities: we are already at the beginning of the line (by chance or because we start at position 0)
		#					 Or we are somewhere in the middle of the line and need to go to the next line
		open(my $mapped_file, '<', $filename) or die "file does not exist\n";
		if($start != 0) {
			seek($mapped_file, $start - 1, 0);
		}
		my $byte_before;
		read($mapped_file, $byte_before, 1) if $start != 0;
		if($byte_before && $byte_before ne '\n') {
			<$mapped_file>; # Just consume this line, the previous chunk will take care of it.
		} 

		while(tell($mapped_file) - 1 < $end) {
			$_ = readline($mapped_file);
			chomp;
			my ($city, $temp) = split(/;/, $_);
			if ($h{$city}) {
				$h{$city}{min} = $temp if $temp < $h{$city}{min};  # min
				$h{$city}{sum} = $h{$city}{sum} += $temp;
				$h{$city}{max} = $temp if $temp > $h{$city}{max};  # max
				$h{$city}{count}++;	    
			} else {
				$h{$city} = {min=>$temp, max=>$temp ,sum=>$temp, count=>1};
			} 
		}

		# Write these measurements to disk as 'measurements_<pid>.txt'
		open(F, '>', "/tmp/measurements_$$.txt") or die "Cannot open file for writing measurements\n";
		print F Dumper(\%h);
		close(F);

		$pm->finish; # Otherwise the child of iteration 1 of this loop will fork 3 times, child with iteration 2 will fork another 2 times, ...
		# We could also just cold exit() here
	} else {
		push @child_pids, $pid;
	}
}

my %complete_hash = ();
if($pid) {
	print "In parent process, waiting for children to finish their work\n";
	$pm->wait_all_children();
	foreach(@child_pids) {
		open(F, '<', "/tmp/measurements_$_.txt") or die "Cannot read measurement file for merging $_\n";
		local $/ = undef;
		eval(Dumper(<F>));
		eval($VAR1);
		my $dumped_data = $VAR1;
		close(F);
		
		foreach my $k (keys(%$dumped_data)) {
			if(exists($complete_hash{$k})) { # We need to merge
				$complete_hash{$k}->{count} += $dumped_data->{$k}->{count};
				$complete_hash{$k}->{sum} += $dumped_data->{$k}->{sum};
				$complete_hash{$k}->{min} = $dumped_data->{$k}->{min} if $dumped_data->{$k}->{min} < $complete_hash{$k}->{min};
				$complete_hash{$k}->{max} = $dumped_data->{$k}->{max} if $dumped_data->{$k}->{max} > $complete_hash{$k}->{max};
			} else { # First time we see this key
				$complete_hash{$k} = $dumped_data->{$k};
			}
		}
	}
	
	my @results;
	for my $city (sort {$a cmp $b} keys %complete_hash){
		my $avg = $complete_hash{$city}{sum}/$complete_hash{$city}{count};
		push @results, "$city=".join('/', map {sprintf("%.1f", $_)} ($complete_hash{$city}{min}, $avg, $complete_hash{$city}{max}));
	   
	}
	say '{'.join(', ', @results).'}';
}
