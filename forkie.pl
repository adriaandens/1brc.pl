use File::Map 'map_file';
use Data::Dumper;
use feature 'say';

use utf8;
binmode(STDOUT, ':encoding(UTF-8)');
binmode(STDIN, ':encoding(UTF-8)');

die "Usage: perl forkie.pl <measurement file>\n" if ! @ARGV;
my $filename = $ARGV[0];
my $processes = 8;
my @child_pids = ();

#map_file(my $mapped_file, $filename, '<');
open(F, "<$filename") or die "file does not exist\n";
# Unsure why but if you stat $mapped_file, it loads the entire file into memory... So use a "normal" oldskool filehandle (returns instantly)
my ($dev,$ino,$mode,$nlink,$uid,$gid,$rdev,$size, $atime,$mtime,$ctime,$blksize,$blocks) = stat F;
close(F);

print "Size of mapped file is $size\n";
my $i = $size / $processes; # Every process does this many lines.

my $pid;
foreach my $p (0..$processes - 1) {
	my $start = int($i * $p);
	my $end = int($i * ($p + 1) - 1);
	$pid = fork();
	if(!$pid) { # Child process
		%h;
		print "$$ Going from $start to $end\n";
		# Do measurements in your range -> actually don't base it upon lines since we don't know where lines start and end...
		# We can't know which offset is a certain line.
		# Better to divide the file into equal byte size chunks and have each process start after the first occurence of a newline (as such you're sure that you start at the beginning of a line)
		open(my $mapped_file, '<', $filename) or die "file does not exist\n";
		if($start != 0) {
			seek($mapped_file, $start - 1, 0);
		}
		my $byte_before;
		read($mapped_file, $byte_before, 1) if $start != 0;
		#print "$$ Previous byte is " . ord($byte_before) . " - $byte_before\n" if $start != 0;
		if($byte_before && $byte_before ne '\n') { # We start at a newline with our chunk
			#print "$$ consume line\n";
			<$mapped_file>; # Just consume this line, the previous chunk will take care of it.
		} 

		print "$$ My current position is: " . tell($mapped_file) . "\n";

		while(tell($mapped_file) - 1 < $end) {
			#print "$$ Current position: " . tell($mapped_file) . " (end for $$ is $end)\n";
			$_ = readline($mapped_file);
			#print "$$ error with readline $!\n";
			if(!$_) {
				print "$$ the dollar variable returned undef...\n";
				last;
			}
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

		last; # Otherwise the child of iteration 1 of this loop will fork 3 times, child with iteration 2 will fork another 2 times, ...
		# We could also just cold exit() here
	} else { # Parent process
		push @child_pids, $pid;
	}
}

my %complete_hash = ();
if($pid) {
	print "In parent process, waiting for children to finish their work\n";
	foreach(1..$processes) {
		my $pid_finished = wait();
		# This PID is now finished so read in the results written to disk
		# Merge the results from this file in the master hash
		# Then sort the keys and print out the result
		print "This child $pid_finished finished\n";

		open(F, '<', "/tmp/measurements_${pid_finished}.txt") or die "Cannot read measurement file for merging ${pid_finished}\n";
		local $/ = undef;
		eval(Dumper(<F>));
		print 'dit zit er in var1: ' . $VAR1;
		eval($VAR1);
		my $dumped_data = $VAR1;
		close(F);
		
		foreach my $k (keys(%$dumped_data)) {
			print "Key $k\n";
			if(exists($complete_hash{$k})) { # We need to merge
				$complete_hash{$k}->{count} += $dumped_data->{$k}->{count};
				$complete_hash{$k}->{sum} += $dumped_data->{$k}->{sum};
				$complete_hash{$k}->{min} = $dumped_data->{$k}->{min} if $dumped_data->{$k}->{min} < $complete_hash{$k}->{min};
				$complete_hash{$k}->{max} = $dumped_data->{$k}->{max} if $dumped_data->{$k}->{max} > $complete_hash{$k}->{max};
			} else { # First time we see this key
				print "$k is not yet in complete hash\n";
				$complete_hash{$k} = $dumped_data->{$k};
			}
		}
	}
	
	my @results;
	for my $city (sort {$a cmp $b} keys %complete_hash){
		my $avg = $complete_hash{$city}{sum}/$complete_hash{$city}{count};
		push @results, "$city=".join('/', map {sprintf("%.1f", $_)} ($complete_hash{$city}{min}, $avg, $complete_hash{$city}{max}));
	   
	}
	say '{'.join(', ',@results).'}';
}

