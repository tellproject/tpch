/*
* $Id: driver.c,v 1.7 2008/09/24 22:35:21 jms Exp $
*
* Revision History
* ===================
* $Log: driver.c,v $
* Revision 1.7  2008/09/24 22:35:21  jms
* remove build number header
*
* Revision 1.6  2008/09/24 22:30:29  jms
* remove build number from default header
*
* Revision 1.5  2008/03/21 17:38:39  jms
* changes for 2.6.3
*
* Revision 1.4  2006/04/26 23:01:10  jms
* address update generation problems
*
* Revision 1.3  2005/10/28 02:54:35  jms
* add release.h changes
*
* Revision 1.2  2005/01/03 20:08:58  jms
* change line terminations
*
* Revision 1.1.1.1  2004/11/24 23:31:46  jms
* re-establish external server
*
* Revision 1.5  2004/04/07 20:17:29  jms
* bug #58 (join fails between order/lineitem)
*
* Revision 1.4  2004/02/18 16:26:49  jms
* 32/64 bit changes for overflow handling needed additional changes when ported back to windows
*
* Revision 1.3  2004/01/22 05:49:29  jms
* AIX porting (AIX 5.1)
*
* Revision 1.2  2004/01/22 03:54:12  jms
* 64 bit support changes for customer address
*
* Revision 1.1.1.1  2003/08/08 21:50:33  jms
* recreation after CVS crash
*
* Revision 1.3  2003/08/08 21:35:26  jms
* first integration of rng64 for o_custkey and l_partkey
*
* Revision 1.2  2003/08/07 17:58:34  jms
* Convery RNG to 64bit space as preparation for new large scale RNG
*
* Revision 1.1.1.1  2003/04/03 18:54:21  jms
* initial checkin
*
*
*/
/* main driver for dss banchmark */

#pragma once

#define DECLARER				/* EXTERN references get defined here */
#define NO_FUNC (int (*) ()) NULL	/* to clean up tdefs */
#define NO_LFUNC (long (*) ()) NULL		/* to clean up tdefs */

#include "config.h"
#include "release.h"
#include <stdlib.h>
#if (defined(_POSIX_)||!defined(WIN32))		/* Change for Windows NT */
#include <unistd.h>
#include <sys/wait.h>
#endif /* WIN32 */
#include <stdio.h>				/* */
#include <limits.h>
#include <math.h>
#include <ctype.h>
#include <signal.h>
#include <string.h>
#include <errno.h>
#ifdef HP
#include <strings.h>
#endif
#if (defined(WIN32)&&!defined(_POSIX_))
#include <process.h>
#pragma warning(disable:4201)
#pragma warning(disable:4214)
#pragma warning(disable:4514)
#define WIN32_LEAN_AND_MEAN
#define NOATOM
#define NOGDICAPMASKS
#define NOMETAFILE
#define NOMINMAX
#define NOMSG
#define NOOPENFILE
#define NORASTEROPS
#define NOSCROLL
#define NOSOUND
#define NOSYSMETRICS
#define NOTEXTMETRIC
#define NOWH
#define NOCOMM
#define NOKANJI
#define NOMCX
#include <windows.h>
#pragma warning(default:4201)
#pragma warning(default:4214)
#endif

#include "dss.h"
#include "dsstypes.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
* Function prototypes
*/
extern void	usage (void);
extern void	kill_load (void);
extern int		pload (int tbl);
extern void	gen_tbl (int tnum, DSS_HUGE start, DSS_HUGE count, long upd_num);
extern int		pr_drange (int tbl, DSS_HUGE min, DSS_HUGE cnt, long num);
extern int		set_files (int t, int pload);
extern int		partial (int, int);
extern void    load_dists (void);


extern int optind, opterr;
extern char *optarg;
#ifdef RNG_TEST
extern seed_t Seed[];
#endif
extern tdef tdefs[];

#ifdef __cplusplus
}
#endif
