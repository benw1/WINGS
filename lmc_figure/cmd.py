import numpy as np
import matplotlib.pyplot as plt
from astropy.io import fits
from mpl_toolkits.axes_grid1 import make_axes_locatable
from astropy.table import Table

plt.rcParams['legend.numpoints'] = 1
plt.rcParams['text.usetex'] = True
plt.rcParams['text.latex.unicode'] = False


def points_inside_poly(points, all_verts):
    """Return bool array of points inside a polygon """
    from matplotlib.path import Path
    return Path(all_verts).contains_points(points)


def emboss(fg='w', lw=3):
    """force embossing"""
    from matplotlib.patheffects import withStroke
    myeffect = withStroke(foreground=fg, linewidth=lw, alpha=0.5)
    return dict(path_effects=[myeffect])


def coolarrow(color='k'):
    """Custom arrow"""
    return dict(arrowstyle='simple', ec="none", color=color, **emboss(lw=2))


def m2M(m, p='vista'):
    """shorter call to mag2Mag for M31"""
    return mag2Mag(m, 'Ks', p, dmod=dmod, Av=Av)


def mag2Mag(mag, filterx, photsys, Av=0., dmod=0.):
    '''
    convert mag to Abs mag using Cardelli et al 1998 extinction curve
    with Rv = 3.1
    '''
    if Av != 0.0:
        Alam_Av = parse_mag_tab(photsys, filterx)
        A = Alam_Av * Av

    return mag-dmod-A


def parse_mag_tab(photsys, filt):
    """return Alambda/Av for a filter given the photsys -- need the file!"""
    tab_mag = './data/tab_mag_{:s}.dat'.format(photsys)
    tab = open(tab_mag, 'r').readlines()
    mags = tab[1].strip().split()
    Alam_Av = np.array(tab[3].strip().split(), dtype=float)
    return Alam_Av[mags.index(filt)]


def annotations(jwst=False):
    """Add text annotations and arrows to plot"""
    # Note: y values are in mags and converted to Abs Mag
    # Milky Way foreground

    red = '#FF0013'  # used for HB, RC
    orange = '#FF8C00'  # used for AGB
    blue = 'navy'  # used for MS
    green = '#138A00'  # used for HeB
    tcolor = 'k'  # used for TRGB
    ccolor = 'k'  # used for MW, Galaxies
    size = 9.5

    ax.annotate(r'$\rm{MW}$',
                xy=(0.35, m2M(13)), xytext=(-0.1, m2M(12)),
                color=ccolor, arrowprops=coolarrow(ccolor),
                size=size, **emboss())
    ax.annotate(r'$\rm{MW}$',
                xy=(0.72, m2M(18)), xytext=(0.9, m2M(17.1)),
                color=ccolor, arrowprops=coolarrow(ccolor),
                size=size, **emboss())
    if jwst:
        xytext=(1.8, 2.46)
    else:
        xytext=(1.3, m2M(22))
    ax.annotate(r'$\rm{Galaxies}$',
                xy=(1.5, m2M(20)), xytext=xytext,
                color=ccolor, arrowprops=coolarrow(ccolor),
                size=size, **emboss())
    ax.annotate(r'$\rm{MS}$',
                xy=(-0.0, m2M(17.8)), xytext=(-0.3, m2M(16.5)),
                color=blue, arrowprops=coolarrow(blue),
                size=size, **emboss())
    ax.annotate(r'$\rm{HB}$',
                xy=(0.3, m2M(17.6)), xytext=(0.0, m2M(16.5)),
                color=red, arrowprops=coolarrow(red),
                size=size, **emboss())
    ax.annotate(r'$\rm{RC}$',
                xy=(0.5, m2M(16.8)), xytext=(0.05, m2M(15.4)),
                color=red, arrowprops=coolarrow(red),
                size=size, **emboss())
    ax.annotate(r'$\rm{HeB}$',
                xy=(0.47, m2M(15.2)), xytext=(0.05, m2M(14)),
                color=green, arrowprops=coolarrow(green),
                size=size, **emboss())
    ax.annotate(r'$\rm{TRGB}$',
                xy=(1.05, m2M(12.2)), xytext=(1.3, m2M(12.7)),
                color=tcolor, arrowprops=coolarrow(tcolor),
                size=size, **emboss())
    ax.annotate(r'$\rm{O\!-\!rich\ AGB}$',
                xy=(1.2, m2M(10.3)), xytext=(0.9, m2M(9)),
                color=orange, arrowprops=coolarrow(orange),
                size=size, **emboss())
    ax.annotate(r'$\rm{C\!-\!rich\ AGB}$',
                xy=(1.7, m2M(11)), xytext=(1.8, m2M(12.7)),
                color=orange, arrowprops=coolarrow(orange),
                size=size, **emboss())
    ax.annotate(r'$\rm{Extreme\ AGB}$',
                xy=(2.5, m2M(10.5)), xytext=(2., m2M(9)),
                color=orange, arrowprops=coolarrow(orange),
                size=size, **emboss())
    if jwst:
        # WD label
        ax.annotate(r'$\rm{WD}$', xy=(-0.2, 9.), xytext=(0, 9.),
                    color='#0EA5FF', arrowprops=coolarrow('#0EA5FF'),
                    size=size, **emboss(), va='center')
        # BD label
        ax.annotate(r'$\rm{BD}$', xy=(1.4, 13.385), xytext=(1.6, 13.385),
                    color='darkred', arrowprops=coolarrow('darkred'),
                    size=size, **emboss(), va='center')
        # MS Kink ~5
        ax.annotate(r'$\rm{MS\ Kink}$', xy=(0.7, 5.), xytext=(.95, 5.),
                    color='darkred', arrowprops=coolarrow('darkred'),
                    size=size, **emboss(), va='center')
    # Distance lines
    # x is padded x limint
    x = ax.get_xlim()[1] - 0.02
    kw = {'color': 'k', 'size': size, 'va': 'bottom', 'ha': 'right'}
    lkw = {'color': 'k', 'alpha': 0.4, 'lw': 1}

    if not jwst:
        tenmpc = -3
        fourmpc = -1
        onempc = 2
        onefiftykpc = 6
    else:
        # From Jason: This Figure appears to be made for the rough F200W = 29
        # (AB mag) limit that JWST reaches in 10,000s (3 hours).
        # For the figure in the paper, I’d like to show cases that are a bit
        # deeper than this...more like 10 or 20 hours.  That will give us ~1
        # magnitude of depth.  For F200W = 30 (AB mag), the K (vega mag) will
        # be 28.2.
        tenmpc = -1.8
        fourmpc = 0.19
        onempc = 3.2
        onefiftykpc = 7.32
        fiftympc = -5.3
        fivekpc = 14.71

    ax.annotate(r'$10\ \rm{Mpc}$', (x, tenmpc), **kw)
    ax.axhline(tenmpc, **lkw)

    ax.annotate(r'$4\ \rm{Mpc}$', (x, fourmpc), **kw)
    ax.axhline(fourmpc, **lkw)

    ax.annotate(r'$1\ \rm{Mpc}$', (x, onempc), **kw)
    ax.axhline(onempc, **lkw)

    ax.axhline(onefiftykpc, **lkw)

    if not jwst:
        ax.annotate(r'$\rm{Galactic\ Satellites}$', (x, mwsats), **kw)
    else:
        ax.annotate(r'$\rm{MW\ Satellites}$', (x, onefiftykpc), **kw)

        ax.annotate(r'$50\ \rm{Mpc}$', (x, fiftympc), **kw)
        ax.axhline(fiftympc, **lkw)

        ax.annotate(r'$\rm{Nearby\ Clusters}$', (x, fivekpc), **kw)
        ax.axhline(fivekpc, **lkw)

    return ax


def loadvmc(inputfile, testphase=False):
    if testphase:
        dat = fits.getdata(inputfile, data_end=5000)
    else:
        dat = fits.getdata(inputfile)
    mag1 = mag2Mag(dat['jAperMag3'], 'J', 'vista', dmod=dmod, Av=Av)
    mag = mag2Mag(dat['ksAperMag3'], 'Ks', 'vista', dmod=dmod, Av=Av)
    cor = mag1 - mag
    return cor, mag


def loadsim(inputfile):
    sgal = Table.read(inputfile, format='ascii.commented_header', guess=False)
    inds, = np.nonzero(sgal['stage'] >= 1)
    smag = mag2Mag(sgal['Ks'][inds], 'Ks', 'vista', dmod=dmod, Av=Av)
    smag1 = mag2Mag(sgal['J'][inds], 'J', 'vista', dmod=dmod, Av=Av)
    scor = smag1 - smag
    return scor, smag


def load2mass(inputfile):
    # 2mass data:
    dat = fits.getdata(inputfile)
    mag1 = mag2Mag(dat['Jmag'], 'J', '2mass', dmod=dmod, Av=Av)
    dmag = mag2Mag(dat['Kmag'], 'Ks', '2mass', dmod=dmod, Av=Av)
    dcor = mag1 - dmag
    return dcor, dmag

def loadwd(inputfile):
    dat = Table.read(inputfile, format='ascii')
    mag1 = mag2Mag(dat['J'], 'J', '2mass', dmod=0., Av=Av)
    dmag = mag2Mag(dat['K'], 'Ks', '2mass', dmod=0., Av=Av)
    dcor = mag1 - dmag
    return dcor, dmag

def loadms(inputfile):
    dat = Table.read(inputfile, format='ascii')
    mag1 = mag2Mag(dat['J'], 'J', '2mass', dmod=0., Av=Av)
    dmag = mag2Mag(dat['K'], 'Ks', '2mass', dmod=0., Av=Av)
    dcor = mag1 - dmag
    return dcor, dmag



def hist2d(vcor, vmag, ax):
    # Compute and plot the 2D histogram
    xmin = -0.5
    xmax = 3
    dx = 0.025

    ymin = mag2Mag(8, 'Ks', 'vista', dmod=dmod, Av=Av)
    ymax = mag2Mag(22, 'Ks', 'vista', dmod=dmod, Av=Av)
    dy = 0.06

    x = vcor
    y = vmag
    H, xbins, ybins = np.histogram2d(vcor, vmag,
                                     bins=(np.arange(xmin, xmax, dx),
                                           np.arange(ymin, ymax, dy)))
    levels = np.linspace(H.min(), H.max(), 15)
    H2 = H / (dx * dy)
    H2[H2 <= .0] = 1  # prevent warnings in log10
    density = np.log10(H2)
    densmax = np.max(density)
    densmin = densmax - 1.8

    cmap = plt.cm.gnuplot
    cmap.set_under(alpha=0.0)
    cmap.set_over('white')
    cmap.set_bad(alpha=0.0)

    x_i = np.digitize(x, xbins) - 1
    y_i = np.digitize(y, ybins) - 1
    x_i[x_i < 0] = 0
    x_i[x_i >= H.shape[0]] = H.shape[0] - 1
    y_i[y_i < 0] = 0
    y_i[y_i >= H.shape[1]] = H.shape[1] - 1
    flag = (H[x_i, y_i] <= levels[1])
    # vmag > m2M(12)
    ax.plot(vcor[flag], vmag[flag], '.', c='black',
            markersize=0.2, zorder=0)

    cbar = ax.imshow(density.T, origin='lower',
                     extent=[xbins[0], xbins[-1], ybins[0], ybins[-1]],
                     cmap=cmap, interpolation='none', aspect='auto',
                     vmin=densmin, vmax=densmax, zorder=1)

    ax.set_xlim(xbins[0], xbins[-1])
    ax.set_ylim(ybins[-1], ybins[0])

    return ax


def plothist(mag, ax, norm=1, bins=None, alpha=1, findnorm=False):
    if bins is None:
        bins = np.arange(mag.min(), mag.max(), 0.1)
    hist, bins = np.histogram(mag, bins=bins)
    ax.plot(hist * norm, bins[:-1], color='k', alpha=alpha)
    # iterated to estimate normalization
    if findnorm:
        try:
            print(np.max(hist), np.argmax(hist), hist[84], bins[84])
        except:
            pass

    return ax


# Globals
jwst = True
dmod = 18.493
Av = 0.04

# Data
vcor, vmag = loadvmc('./data/ADP.2011-09-22T15:44:49.967.fits')
tcor, tmag = load2mass('./data/asu.fit')

fig, ax = plt.subplots(figsize=(5., 3.75))
# elements with zorder<2 will be rasterized
# ax.set_rasterization_zorder(1)

# Vista data
ax = hist2d(vcor, vmag, ax)

# 2mass data
ax.plot(tcor[tmag < m2M(12, p='2mass')], tmag[tmag < m2M(12, p='2mass')], '.',
        c='black', markersize=0.4, zorder=0)

if not jwst:
    scor, smag = loadsim('./data/wfirstsm.dat')
    ax.plot(scor[smag > m2M(10)], smag[smag > m2M(10)], '.',
        c='grey', markersize=0.4, zorder=0, alpha=0.3)
    divider = make_axes_locatable(ax)
    ax1 = divider.append_axes("right", 1., pad=0, sharey=ax)

    # Iterated to find best normalization (arb mass simulation)...
    norm = 20269 / 893.
    bins = np.arange(1.8, smag.max(), 0.1)

    ax1 = plothist(smag, ax1, norm=norm, bins=bins, alpha=0.4, findnorm=False)

    # Cut MW and Galaxies from LF
    verts = [[-0.5, 6.5], [-0.5, m2M(12)], [3, m2M(12)],
             [3, -2.9], [.65, -2.9], [0.65, 6.5], [-0.5, 6.5]]

    inds = points_inside_poly(np.column_stack([vcor, vmag]), verts)

    ax1 = plothist(vmag[inds], ax1)
    ax1 = plothist(tmag[[tmag <= m2M(12.1, p='2mass')]], ax1)
    ax1.tick_params(labelleft=False, labelsize=10)
    ax1.set_xlim(10, ax1.get_xlim()[1])
    ax1.set_xscale('log')
    ax.set_ylim(6.5, ax.get_ylim()[1])
    ax.set_ylabel(r'$K_{\rm{s}}$')
else:
    # WD Cooling curve
    wcor, wmag = loadwd('./data/Table_Mass_0.5.dat.5')
    ax.plot(wcor, wmag, c='#0EA5FF', lw=2.3)

    # MS and Brown dwarfs
    bcor, bmag = loadms('./data/all47_JC11.data')
    ax.plot(bcor[bmag > 1], bmag[bmag > 1], c='darkred', lw=2.3, zorder=0)

    ax.set_ylabel(r'$K_{\rm{s}}\ \rm{(Vega\ mag)}$')
    ax.set_ylim(16., -11)

annotations(jwst=jwst)

ax.set_xlabel(r'$J-K_{\rm{s}}$')

ax.tick_params(labelsize=10)

fig.subplots_adjust(hspace=0.0, wspace=0.0)
plt.savefig('vmc_sep.png', bbox_inches='tight', dpi=300)
plt.savefig('vmc_sep_r2.pdf', bbox_inches='tight', dpi=300)
