import javax.swing.*;
import javax.swing.border.EmptyBorder;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import javax.swing.table.DefaultTableModel;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.io.File;
import java.io.FileInputStream;
import java.nio.file.*;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * File Search Indexer - Java (Swing + SQLite)
 *
 * Features:
 * - Recursive directory scanner collecting: path, name, extension, size, lastModified, indexedAt, optional SHA-256 hash
 * - SQLite index (index.db) with efficient search, filters (name/extension, size, date), sorting, pagination
 * - GUI (Swing): choose folder, scan, search with filters, sort, page controls
 * - "Recently added" quick filter
 * - "Duplicate finder" (by size + hash; hash computed only if enabled during scan)
 * - Robust error handling and non-blocking background tasks
 */
public class FileSearchIndexer extends JFrame {
    // ===== Utilities =====
    static final String DB_URL = "jdbc:sqlite:index.db";
    static final SimpleDateFormat UI_DATE = new SimpleDateFormat("yyyy-MM-dd");
    static final DateTimeFormatter DT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // ===== Data Model =====
    static class FileRecord {
        long id; // rowid
        String path;
        String name;
        String extension;
        long size;
        long lastModified; // epoch millis
        long indexedAt; // epoch millis
        String sha256; // optional

        public Object[] toTableRow() {
            return new Object[]{
                    id,
                    name,
                    extension,
                    humanSize(size),
                    formatTs(lastModified),
                    formatTs(indexedAt),
                    path
            };
        }
    }

    // ===== Persistence Layer =====
    static class Database implements AutoCloseable {
        private final Connection conn;

        Database() throws SQLException {
            conn = DriverManager.getConnection(DB_URL);
            conn.setAutoCommit(false);
            init();
        }

        private void init() throws SQLException {
            try (Statement st = conn.createStatement()) {
                st.execute("PRAGMA journal_mode=WAL");
                st.execute("PRAGMA synchronous=NORMAL");
                st.execute("CREATE TABLE IF NOT EXISTS files (\n" +
                        "  id INTEGER PRIMARY KEY,\n" +
                        "  path TEXT UNIQUE,\n" +
                        "  name TEXT,\n" +
                        "  extension TEXT,\n" +
                        "  size INTEGER,\n" +
                        "  last_modified INTEGER,\n" +
                        "  indexed_at INTEGER,\n" +
                        "  sha256 TEXT\n" +
                        ")");
                st.execute("CREATE INDEX IF NOT EXISTS idx_files_name ON files(name)");
                st.execute("CREATE INDEX IF NOT EXISTS idx_files_ext ON files(extension)");
                st.execute("CREATE INDEX IF NOT EXISTS idx_files_size ON files(size)");
                st.execute("CREATE INDEX IF NOT EXISTS idx_files_lastmod ON files(last_modified)");
                st.execute("CREATE INDEX IF NOT EXISTS idx_files_sha ON files(sha256)");
            }
            conn.commit();
        }

        public void upsert(FileRecord r) throws SQLException {
            String sql = "INSERT INTO files(path,name,extension,size,last_modified,indexed_at,sha256)\n" +
                    "VALUES(?,?,?,?,?,?,?)\n" +
                    "ON CONFLICT(path) DO UPDATE SET name=excluded.name, extension=excluded.extension, size=excluded.size,\n" +
                    " last_modified=excluded.last_modified, indexed_at=excluded.indexed_at, sha256=COALESCE(excluded.sha256, files.sha256)";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, r.path);
                ps.setString(2, r.name);
                ps.setString(3, r.extension);
                ps.setLong(4, r.size);
                ps.setLong(5, r.lastModified);
                ps.setLong(6, r.indexedAt);
                if (r.sha256 == null) ps.setNull(7, Types.VARCHAR);
                else ps.setString(7, r.sha256);
                ps.executeUpdate();
            }
        }

        public List<FileRecord> search(String nameLike, String ext, Long minSize, Long maxSize,
                                       Long minDate, Long maxDate, String orderBy, boolean desc, int limit, int offset) throws SQLException {
            StringBuilder sb = new StringBuilder("SELECT id,path,name,extension,size,last_modified,indexed_at,sha256 FROM files WHERE 1=1");
            List<Object> params = new ArrayList<>();
            if (nameLike != null && !nameLike.isEmpty()) {
                sb.append(" AND name LIKE ?");
                params.add("%" + nameLike + "%");
            }
            if (ext != null && !ext.isEmpty()) {
                sb.append(" AND extension = ?");
                params.add(ext.toLowerCase());
            }
            if (minSize != null) { sb.append(" AND size >= ?"); params.add(minSize); }
            if (maxSize != null) { sb.append(" AND size <= ?"); params.add(maxSize); }
            if (minDate != null) { sb.append(" AND last_modified >= ?"); params.add(minDate); }
            if (maxDate != null) { sb.append(" AND last_modified <= ?"); params.add(maxDate); }
            if (orderBy == null || orderBy.isEmpty()) orderBy = "name";
            sb.append(" ORDER BY ").append(orderBy).append(desc ? " DESC" : " ASC");
            sb.append(" LIMIT ? OFFSET ?");
            params.add(limit);
            params.add(offset);

            try (PreparedStatement ps = conn.prepareStatement(sb.toString())) {
                for (int i = 0; i < params.size(); i++) {
                    Object p = params.get(i);
                    if (p instanceof String) ps.setString(i + 1, (String) p);
                    else if (p instanceof Long) ps.setLong(i + 1, (Long) p);
                    else throw new IllegalArgumentException("Unsupported param type: " + p);
                }
                ResultSet rs = ps.executeQuery();
                List<FileRecord> list = new ArrayList<>();
                while (rs.next()) {
                    FileRecord r = new FileRecord();
                    r.id = rs.getLong(1);
                    r.path = rs.getString(2);
                    r.name = rs.getString(3);
                    r.extension = rs.getString(4);
                    r.size = rs.getLong(5);
                    r.lastModified = rs.getLong(6);
                    r.indexedAt = rs.getLong(7);
                    r.sha256 = rs.getString(8);
                    list.add(r);
                }
                return list;
            }
        }

        public List<FileRecord> recent(int limit) throws SQLException {
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT id,path,name,extension,size,last_modified,indexed_at,sha256 FROM files ORDER BY indexed_at DESC LIMIT ?")) {
                ps.setInt(1, limit);
                ResultSet rs = ps.executeQuery();
                List<FileRecord> list = new ArrayList<>();
                while (rs.next()) {
                    FileRecord r = new FileRecord();
                    r.id = rs.getLong(1);
                    r.path = rs.getString(2);
                    r.name = rs.getString(3);
                    r.extension = rs.getString(4);
                    r.size = rs.getLong(5);
                    r.lastModified = rs.getLong(6);
                    r.indexedAt = rs.getLong(7);
                    r.sha256 = rs.getString(8);
                    list.add(r);
                }
                return list;
            }
        }

        public List<FileRecord> duplicates() throws SQLException {
            // Duplicate by same size AND same hash (hash may be NULL; require not null)
            String sql = "SELECT f.id,f.path,f.name,f.extension,f.size,f.last_modified,f.indexed_at,f.sha256\n" +
                    " FROM files f JOIN (SELECT sha256, size, COUNT(*) c FROM files WHERE sha256 IS NOT NULL GROUP BY sha256,size HAVING c>1) d\n" +
                    " ON f.sha256=d.sha256 AND f.size=d.size ORDER BY d.size DESC, f.name";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ResultSet rs = ps.executeQuery();
                List<FileRecord> list = new ArrayList<>();
                while (rs.next()) {
                    FileRecord r = new FileRecord();
                    r.id = rs.getLong(1);
                    r.path = rs.getString(2);
                    r.name = rs.getString(3);
                    r.extension = rs.getString(4);
                    r.size = rs.getLong(5);
                    r.lastModified = rs.getLong(6);
                    r.indexedAt = rs.getLong(7);
                    r.sha256 = rs.getString(8);
                    list.add(r);
                }
                return list;
            }
        }

        public void commit() throws SQLException { conn.commit(); }

        @Override public void close() {
            try { conn.commit(); } catch (Exception ignored) {}
            try { conn.close(); } catch (Exception ignored) {}
        }
    }

    // ===== Indexer (Recursive Scanner) =====
    static class Indexer {
        private final boolean computeHash;
        private final Database db;
        private final JLabel status;
        private final ThreadPoolExecutor pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(Math.max(2, Runtime.getRuntime().availableProcessors()-1));
        private volatile long filesSeen = 0;

        Indexer(Database db, boolean computeHash, JLabel status) {
            this.db = db; this.computeHash = computeHash; this.status = status;
        }

        public void scan(Path root) {
            filesSeen = 0;
            status.setText("Scanning…");
            long started = System.currentTimeMillis();
            try {
                Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
                    @Override public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                        if (attrs.isRegularFile()) {
                            pool.submit(() -> indexOne(file.toFile(), attrs));
                        }
                        return FileVisitResult.CONTINUE;
                    }

                    @Override public FileVisitResult visitFileFailed(Path file, IOException exc) {
                        // Skip broken or permission-denied files
                        return FileVisitResult.CONTINUE;
                    }
                });
            } catch (Exception e) {
                SwingUtilities.invokeLater(() -> JOptionPane.showMessageDialog(null, "Scan failed: " + e.getMessage(), "Error", JOptionPane.ERROR_MESSAGE));
            }

            pool.shutdown();
            try { pool.awaitTermination(365, TimeUnit.DAYS); } catch (InterruptedException ignored) {}
            try { db.commit(); } catch (SQLException ignored) {}
            long dur = System.currentTimeMillis() - started;
            status.setText("Scan finished. Files indexed: " + filesSeen + " in " + dur + " ms");
        }

        private void indexOne(File f, BasicFileAttributes attrs) {
            try {
                FileRecord r = new FileRecord();
                r.path = f.getAbsolutePath();
                r.name = f.getName();
                r.extension = getExtension(f.getName());
                r.size = attrs.size();
                r.lastModified = attrs.lastModifiedTime().toMillis();
                r.indexedAt = System.currentTimeMillis();
                if (computeHash) r.sha256 = sha256(f);
                db.upsert(r);
                long n = ++filesSeen;
                if (n % 500 == 0) db.commit();
                if (n % 200 == 0) status.setText("Indexed " + n + " files…");
            } catch (Exception ignored) { }
        }

        private static String sha256(File f) {
            try (DigestInputStream dis = new DigestInputStream(new FileInputStream(f), MessageDigest.getInstance("SHA-256"))) {
                byte[] buf = new byte[8192];
                while (dis.read(buf) != -1) { /* stream */ }
                byte[] hash = dis.getMessageDigest().digest();
                StringBuilder sb = new StringBuilder();
                for (byte b : hash) sb.append(String.format("%02x", b));
                return sb.toString();
            } catch (Exception e) {
                return null; // can't hash -> leave null
            }
        }
    }

    // ===== GUI =====
    private JTextField txtFolder;
    private JCheckBox chkHash;
    private JButton btnScan;

    private JTextField txtName;
    private JTextField txtExt;
    private JTextField txtSizeMin;
    private JTextField txtSizeMax;
    private JTextField txtDateFrom;
    private JTextField txtDateTo;
    private JComboBox<String> cmbSort;
    private JCheckBox chkDesc;
    private JSpinner spnLimit;
    private JButton btnPrev, btnNext, btnSearch, btnRecent, btnDupes;
    private JLabel lblPage;

    private JTable table;
    private DefaultTableModel model;
    private JLabel status;

    private int page = 0;

    public FileSearchIndexer() {
        super("File Search Indexer (Java + SQLite)");
        setDefaultCloseOperation(EXIT_ON_CLOSE);
        setSize(1100, 720);
        setLocationRelativeTo(null);
        setLayout(new BorderLayout());

        add(buildTopPanel(), BorderLayout.NORTH);
        add(buildCenterPanel(), BorderLayout.CENTER);
        add(buildStatusPanel(), BorderLayout.SOUTH);

        // Warm-up DB
        try { Class.forName("org.sqlite.JDBC"); } catch (Exception ignored) {}
    }

    private JPanel buildTopPanel() {
        JPanel top = new JPanel(new BorderLayout());
        top.setBorder(new EmptyBorder(10,10,10,10));

        // Scan panel
        JPanel scan = new JPanel(new FlowLayout(FlowLayout.LEFT));
        txtFolder = new JTextField(40);
        JButton btnBrowse = new JButton("Browse…");
        btnBrowse.addActionListener(e -> onBrowse());
        chkHash = new JCheckBox("Compute SHA-256 (slower, needed for accurate duplicates)");
        btnScan = new JButton("Scan & Index");
        btnScan.addActionListener(this::onScan);
        scan.add(new JLabel("Folder:"));
        scan.add(txtFolder);
        scan.add(btnBrowse);
        scan.add(chkHash);
        scan.add(btnScan);

        // Search panel
        JPanel search = new JPanel(new FlowLayout(FlowLayout.LEFT));
        txtName = new JTextField(12);
        txtExt = new JTextField(6);
        txtSizeMin = new JTextField(6);
        txtSizeMax = new JTextField(6);
        txtDateFrom = new JTextField(9);
        txtDateTo = new JTextField(9);
        cmbSort = new JComboBox<>(new String[]{"name","extension","size","last_modified","indexed_at"});
        chkDesc = new JCheckBox("Desc");
        spnLimit = new JSpinner(new SpinnerNumberModel(50, 10, 5000, 10));
        btnPrev = new JButton("Prev");
        btnNext = new JButton("Next");
        btnSearch = new JButton("Search");
        btnRecent = new JButton("Recently added");
        btnDupes = new JButton("Find duplicates");
        lblPage = new JLabel("Page 1");

        btnPrev.addActionListener(e -> { if (page>0){ page--; doSearch(); } });
        btnNext.addActionListener(e -> { page++; doSearch(); });
        btnSearch.addActionListener(e -> { page=0; doSearch(); });
        btnRecent.addActionListener(e -> showRecent());
        btnDupes.addActionListener(e -> showDupes());

        search.add(new JLabel("Name:")); search.add(txtName);
        search.add(new JLabel("Ext:")); search.add(txtExt);
        search.add(new JLabel("Size ≥:")); search.add(txtSizeMin);
        search.add(new JLabel("Size ≤:")); search.add(txtSizeMax);
        search.add(new JLabel("Date from:")); search.add(txtDateFrom);
        search.add(new JLabel("to:")); search.add(txtDateTo);
        search.add(new JLabel("Sort:")); search.add(cmbSort); search.add(chkDesc);
        search.add(new JLabel("Limit:")); search.add(spnLimit);
        search.add(btnPrev); search.add(btnNext); search.add(lblPage);
        search.add(btnSearch); search.add(btnRecent); search.add(btnDupes);

        // Quick hint for date format
        JLabel hint = new JLabel(" Date format: yyyy-MM-dd ");
        hint.setForeground(Color.DARK_GRAY);
        search.add(hint);

        // Live search on name field
        txtName.getDocument().addDocumentListener(new DocumentListener() {
            @Override public void insertUpdate(DocumentEvent e) { debounceSearch(); }
            @Override public void removeUpdate(DocumentEvent e) { debounceSearch(); }
            @Override public void changedUpdate(DocumentEvent e) { debounceSearch(); }
        });

        top.add(scan, BorderLayout.NORTH);
        top.add(search, BorderLayout.SOUTH);
        return top;
    }

    private void debounceSearch() {
        // simple debounce using timer
        Timer t = new Timer(300, e -> { page=0; doSearch(); });
        t.setRepeats(false); t.start();
    }

    private JScrollPane buildCenterPanel() {
        model = new DefaultTableModel(new Object[]{"ID","Name","Ext","Size","Last Modified","Indexed At","Path"}, 0) {
            @Override public boolean isCellEditable(int row, int column) { return false; }
        };
        table = new JTable(model);
        table.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
        table.getColumnModel().getColumn(0).setPreferredWidth(60);
        table.getColumnModel().getColumn(1).setPreferredWidth(220);
        table.getColumnModel().getColumn(2).setPreferredWidth(60);
        table.getColumnModel().getColumn(3).setPreferredWidth(100);
        table.getColumnModel().getColumn(4).setPreferredWidth(160);
        table.getColumnModel().getColumn(5).setPreferredWidth(160);
        table.getColumnModel().getColumn(6).setPreferredWidth(400);
        return new JScrollPane(table);
    }

    private JPanel buildStatusPanel() {
        JPanel p = new JPanel(new BorderLayout());
        status = new JLabel("Ready.");
        p.add(status, BorderLayout.WEST);
        return p;
    }

    private void onBrowse() {
        JFileChooser ch = new JFileChooser();
        ch.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
        if (ch.showOpenDialog(this) == JFileChooser.APPROVE_OPTION) {
            txtFolder.setText(ch.getSelectedFile().getAbsolutePath());
        }
    }

    private void onScan(ActionEvent e) {
        String folder = txtFolder.getText().trim();
        if (folder.isEmpty()) { JOptionPane.showMessageDialog(this, "Select a folder first."); return; }
        Path root = Paths.get(folder);
        if (!Files.isDirectory(root)) { JOptionPane.showMessageDialog(this, "Not a directory."); return; }
        btnScan.setEnabled(false);
        status.setText("Starting scan…");
        new Thread(() -> {
            try (Database db = new Database()) {
                new Indexer(db, chkHash.isSelected(), status).scan(root);
            } catch (Exception ex) {
                SwingUtilities.invokeLater(() -> JOptionPane.showMessageDialog(this, "Error: " + ex.getMessage(), "Error", JOptionPane.ERROR_MESSAGE));
            } finally {
                SwingUtilities.invokeLater(() -> btnScan.setEnabled(true));
            }
        }, "scan-thread").start();
    }

    private void doSearch() {
        int limit = (Integer) spnLimit.getValue();
        int offset = page * limit;
        lblPage.setText("Page " + (page+1));
        status.setText("Searching…");
        new Thread(() -> {
            try (Database db = new Database()) {
                List<FileRecord> list = db.search(
                        emptyToNull(txtName.getText()),
                        normalizeExt(txtExt.getText()),
                        parseLong(txtSizeMin.getText()),
                        parseLong(txtSizeMax.getText()),
                        parseDate(txtDateFrom.getText()),
                        parseDate(txtDateTo.getText()),
                        (String) cmbSort.getSelectedItem(),
                        chkDesc.isSelected(),
                        limit,
                        offset
                );
                SwingUtilities.invokeLater(() -> fillTable(list));
                status.setText("Results: " + list.size());
            } catch (Exception ex) {
                SwingUtilities.invokeLater(() -> JOptionPane.showMessageDialog(this, "Search error: " + ex.getMessage(), "Error", JOptionPane.ERROR_MESSAGE));
            }
        }, "search-thread").start();
    }

    private void showRecent() {
        status.setText("Loading recent…");
        new Thread(() -> {
            try (Database db = new Database()) {
                List<FileRecord> list = db.recent((Integer) spnLimit.getValue());
                SwingUtilities.invokeLater(() -> fillTable(list));
                status.setText("Recent loaded: " + list.size());
            } catch (Exception ex) {
                SwingUtilities.invokeLater(() -> JOptionPane.showMessageDialog(this, "Error: " + ex.getMessage(), "Error", JOptionPane.ERROR_MESSAGE));
            }
        }, "recent-thread").start();
    }

    private void showDupes() {
        status.setText("Finding duplicates…\n(Tip: run scan with hashing enabled)");
        new Thread(() -> {
            try (Database db = new Database()) {
                List<FileRecord> list = db.duplicates();
                SwingUtilities.invokeLater(() -> fillTable(list));
                status.setText("Duplicates: " + list.size());
            } catch (Exception ex) {
                SwingUtilities.invokeLater(() -> JOptionPane.showMessageDialog(this, "Dupes error: " + ex.getMessage(), "Error", JOptionPane.ERROR_MESSAGE));
            }
        }, "dupes-thread").start();
    }

    private void fillTable(List<FileRecord> rows) {
        model.setRowCount(0);
        for (FileRecord r : rows) model.addRow(r.toTableRow());
    }

    // ===== Helpers =====
    static String emptyToNull(String s) { s = s == null ? null : s.trim(); return (s == null || s.isEmpty()) ? null : s; }

    static String normalizeExt(String ext) {
        if (ext == null) return null;
        ext = ext.trim();
        if (ext.isEmpty()) return null;
        if (ext.startsWith(".")) ext = ext.substring(1);
        return ext.toLowerCase();
    }

    static Long parseLong(String s) {
        try { s = s.trim(); if (s.isEmpty()) return null; return Long.parseLong(s); } catch (Exception e) { return null; }
    }

    static Long parseDate(String s) {
        try { s = s.trim(); if (s.isEmpty()) return null; return UI_DATE.parse(s).getTime(); } catch (Exception e) { return null; }
    }

    static String humanSize(long bytes) {
        if (bytes < 1024) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(1024));
        String pre = "KMGTPE".charAt(exp - 1) + "";
        return String.format(Locale.US, "%.1f %sB", bytes / Math.pow(1024, exp), pre);
    }

    static String formatTs(long ms) {
        return Instant.ofEpochMilli(ms).atZone(ZoneId.systemDefault()).format(DT);
    }

    static String getExtension(String name) {
        int idx = name.lastIndexOf('.');
        if (idx == -1 || idx == name.length()-1) return "";
        return name.substring(idx+1).toLowerCase();
    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> new FileSearchIndexer().setVisible(true));
    }
}
