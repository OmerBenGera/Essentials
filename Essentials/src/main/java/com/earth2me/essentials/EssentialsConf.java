package com.earth2me.essentials;

import com.google.common.io.Files;
import net.ess3.api.InvalidWorldException;
import org.bukkit.Location;
import org.bukkit.Material;
import org.bukkit.Server;
import org.bukkit.World;
import org.bukkit.configuration.ConfigurationSection;
import org.bukkit.configuration.InvalidConfigurationException;
import org.bukkit.configuration.file.YamlConfiguration;
import org.bukkit.enchantments.Enchantment;
import org.bukkit.inventory.ItemStack;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.math.BigDecimal;
import java.math.MathContext;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.earth2me.essentials.I18n.tl;

public class EssentialsConf extends YamlConfiguration {
    protected static final Logger LOGGER = Logger.getLogger("Essentials");
    protected static final Charset UTF8 = StandardCharsets.UTF_8;
    private static final ExecutorService EXECUTOR_SERVICE = Executors.newSingleThreadExecutor();
    protected final File configFile;
    private Future<?> writeTask = null;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final AtomicBoolean transaction = new AtomicBoolean(false);
    private final byte[] bytebuffer = new byte[1024];
    protected String templateName = null;
    private Class<?> resourceClass = EssentialsConf.class;

    public EssentialsConf(final File configFile) {
        super();
        this.configFile = configFile.getAbsoluteFile();
    }

    public static BigDecimal toBigDecimal(final String input, final BigDecimal def) {
        if (input == null || input.isEmpty()) {
            return def;
        } else {
            try {
                return new BigDecimal(input, MathContext.DECIMAL128);
            } catch (final NumberFormatException | ArithmeticException e) {
                return def;
            }
        }
    }

    public synchronized void load() {
        if (writeTask != null) {
            LOGGER.log(Level.INFO, "File {0} not read, because it''s not yet written to disk.", configFile);
            return;
        }
        if (!configFile.getParentFile().exists()) {
            if (!configFile.getParentFile().mkdirs()) {
                LOGGER.log(Level.SEVERE, tl("failedToCreateConfig", configFile.toString()));
            }
        }
        // This will delete files where the first character is 0. In most cases they are broken.
        if (configFile.exists() && configFile.length() != 0) {
            try {
                final InputStream input = new FileInputStream(configFile);
                try {
                    if (input.read() == 0) {
                        input.close();
                        configFile.delete();
                    }
                } catch (final IOException ex) {
                    LOGGER.log(Level.SEVERE, null, ex);
                } finally {
                    try {
                        input.close();
                    } catch (final IOException ex) {
                        LOGGER.log(Level.SEVERE, null, ex);
                    }
                }
            } catch (final FileNotFoundException ex) {
                LOGGER.log(Level.SEVERE, null, ex);
            }
        }

        if (!configFile.exists()) {
            if (legacyFileExists()) {
                convertLegacyFile();
            } else if (altFileExists()) {
                convertAltFile();
            } else if (templateName != null) {
                LOGGER.log(Level.INFO, tl("creatingConfigFromTemplate", configFile.toString()));
                createFromTemplate();
            } else {
                return;
            }
        }

        try {
            try (final FileInputStream inputStream = new FileInputStream(configFile)) {
                final long startSize = configFile.length();
                if (startSize > Integer.MAX_VALUE) {
                    throw new InvalidConfigurationException("File too big");
                }
                ByteBuffer buffer = ByteBuffer.allocate((int) startSize);
                int length;
                while ((length = inputStream.read(bytebuffer)) != -1) {
                    if (length > buffer.remaining()) {
                        final ByteBuffer resize = ByteBuffer.allocate(buffer.capacity() + length - buffer.remaining());
                        final int resizePosition = buffer.position();
                        // Fix builds compiled against Java 9+ breaking on Java 8
                        ((Buffer) buffer).rewind();
                        resize.put(buffer);
                        resize.position(resizePosition);
                        buffer = resize;
                    }
                    buffer.put(bytebuffer, 0, length);
                }
                ((Buffer) buffer).rewind();
                final CharBuffer data = CharBuffer.allocate(buffer.capacity());
                CharsetDecoder decoder = UTF8.newDecoder();
                CoderResult result = decoder.decode(buffer, data, true);
                if (result.isError()) {
                    ((Buffer) buffer).rewind();
                    ((Buffer) data).clear();
                    LOGGER.log(Level.INFO, "File " + configFile.getAbsolutePath() + " is not utf-8 encoded, trying " + Charset.defaultCharset().displayName());
                    decoder = Charset.defaultCharset().newDecoder();
                    result = decoder.decode(buffer, data, true);
                    if (result.isError()) {
                        throw new InvalidConfigurationException("Invalid Characters in file " + configFile.getAbsolutePath());
                    } else {
                        decoder.flush(data);
                    }
                } else {
                    decoder.flush(data);
                }
                final int end = data.position();
                ((Buffer) data).rewind();
                super.loadFromString(data.subSequence(0, end).toString());
            }
        } catch (final IOException ex) {
            LOGGER.log(Level.SEVERE, ex.getMessage(), ex);
        } catch (final InvalidConfigurationException ex) {
            final File broken = new File(configFile.getAbsolutePath() + ".broken." + System.currentTimeMillis());
            configFile.renameTo(broken);
            LOGGER.log(Level.SEVERE, "The file " + configFile.toString() + " is broken, it has been renamed to " + broken.toString(), ex.getCause());
        }
    }

    public boolean legacyFileExists() {
        return false;
    }

    public void convertLegacyFile() {
        LOGGER.log(Level.SEVERE, "Unable to import legacy config file.");
    }

    public boolean altFileExists() {
        return false;
    }

    public void convertAltFile() {
        LOGGER.log(Level.SEVERE, "Unable to import alt config file.");
    }

    private void createFromTemplate() {
        InputStream istr = null;
        OutputStream ostr = null;
        try {
            istr = resourceClass.getResourceAsStream(templateName);
            if (istr == null) {
                LOGGER.log(Level.SEVERE, tl("couldNotFindTemplate", templateName));
                return;
            }
            ostr = new FileOutputStream(configFile);
            final byte[] buffer = new byte[1024];
            int length = 0;
            length = istr.read(buffer);
            while (length > 0) {
                ostr.write(buffer, 0, length);
                length = istr.read(buffer);
            }
        } catch (final IOException ex) {
            LOGGER.log(Level.SEVERE, tl("failedToWriteConfig", configFile.toString()), ex);
        } finally {
            try {
                if (istr != null) {
                    istr.close();
                }
            } catch (final IOException ex) {
                Logger.getLogger(EssentialsConf.class.getName()).log(Level.SEVERE, null, ex);
            }
            try {
                if (ostr != null) {
                    ostr.close();
                }
            } catch (final IOException ex) {
                LOGGER.log(Level.SEVERE, tl("failedToCloseConfig", configFile.toString()), ex);
            }
        }
    }

    public void setTemplateName(final String templateName) {
        this.templateName = templateName;
    }

    public File getFile() {
        return configFile;
    }

    public void setTemplateName(final String templateName, final Class<?> resClass) {
        this.templateName = templateName;
        this.resourceClass = resClass;
    }

    public void startTransaction() {
        transaction.set(true);
    }

    public void stopTransaction() {
        transaction.set(false);
        save();
    }

    public void save() {
        try {
            save(configFile);
        } catch (final IOException ex) {
            LOGGER.log(Level.SEVERE, ex.getMessage(), ex);
        }
    }

    public void saveWithError() throws IOException {
        save(configFile);
    }

    @Override
    public void save(final File file) throws IOException {
        if (!transaction.get()) {
            delayedSave(file);
        }
    }

    //This may be aborted if there are stagnant requests sitting in queue.
    //This needs fixed to discard outstanding save requests.
    public void forceSave() {
        try {
            final Future<?> future = delayedSave(configFile);
            if (future != null) {
                future.get();
            }
        } catch (final InterruptedException | ExecutionException ex) {
            LOGGER.log(Level.SEVERE, ex.getMessage(), ex);
        }
    }

    public void cleanup() {
        forceSave();
    }

    private Future<?> delayedSave(final File file) {
        if (file == null) {
            throw new IllegalArgumentException("File cannot be null");
        }

        if(writeTask == null)
            writeTask = EXECUTOR_SERVICE.submit(new WriteRunner());

        return writeTask;
    }

    public boolean hasProperty(final String path) {
        return isSet(path);
    }

    public Location getLocation(final String path, final Server server) throws InvalidWorldException {
        final String worldString = (path == null ? "" : path + ".") + "world";
        final String worldName = getString(worldString);
        if (worldName == null || worldName.isEmpty()) {
            return null;
        }
        final World world = server.getWorld(worldName);
        if (world == null) {
            throw new InvalidWorldException(worldName);
        }
        return new Location(world, getDouble((path == null ? "" : path + ".") + "x", 0), getDouble((path == null ? "" : path + ".") + "y", 0), getDouble((path == null ? "" : path + ".") + "z", 0), (float) getDouble((path == null ? "" : path + ".") + "yaw", 0), (float) getDouble((path == null ? "" : path + ".") + "pitch", 0));
    }

    public void setProperty(final String path, final Location loc) {
        set((path == null ? "" : path + ".") + "world", loc.getWorld().getName());
        set((path == null ? "" : path + ".") + "x", loc.getX());
        set((path == null ? "" : path + ".") + "y", loc.getY());
        set((path == null ? "" : path + ".") + "z", loc.getZ());
        set((path == null ? "" : path + ".") + "yaw", loc.getYaw());
        set((path == null ? "" : path + ".") + "pitch", loc.getPitch());
    }

    @Override
    public ItemStack getItemStack(final String path) {
        final ItemStack stack = new ItemStack(Material.valueOf(getString(path + ".type", "AIR")), getInt(path + ".amount", 1), (short) getInt(path + ".damage", 0));
        final ConfigurationSection enchants = getConfigurationSection(path + ".enchant");
        if (enchants != null) {
            for (final String enchant : enchants.getKeys(false)) {
                final Enchantment enchantment = Enchantment.getByName(enchant.toUpperCase(Locale.ENGLISH));
                if (enchantment == null) {
                    continue;
                }
                final int level = getInt(path + ".enchant." + enchant, enchantment.getStartLevel());
                stack.addUnsafeEnchantment(enchantment, level);
            }
        }
        return stack;
        /*
         * ,
         * (byte)getInt(path + ".data", 0)
         */
    }

    public BigDecimal getBigDecimal(final String path, final BigDecimal def) {
        final String input = super.getString(path);
        return toBigDecimal(input, def);
    }

    public void setProperty(final String path, final ItemStack stack) {
        final Map<String, Object> map = new HashMap<>();
        map.put("type", stack.getType().toString());
        map.put("amount", stack.getAmount());
        map.put("damage", stack.getDurability());
        final Map<Enchantment, Integer> enchantments = stack.getEnchantments();
        if (!enchantments.isEmpty()) {
            final Map<String, Integer> enchant = new HashMap<>();
            for (final Map.Entry<Enchantment, Integer> entry : enchantments.entrySet()) {
                enchant.put(entry.getKey().getName().toLowerCase(Locale.ENGLISH), entry.getValue());
            }
            map.put("enchant", enchant);
        }
        // getData().getData() is broken
        //map.put("data", stack.getDurability());
        set(path, map);
    }

    public void setProperty(final String path, final List object) {
        set(path, new ArrayList(object));
    }

    public void setProperty(final String path, final Map object) {
        set(path, new LinkedHashMap(object));
    }

    public Object getProperty(final String path) {
        return get(path);
    }

    public void setProperty(final String path, final BigDecimal bigDecimal) {
        set(path, bigDecimal.toString());
    }

    public void setProperty(final String path, final Object object) {
        set(path, object);
    }

    public void removeProperty(final String path) {
        set(path, null);
    }

    @Override
    public Object get(final String path, final Object def) {
        // There's no need to wrap each get method, and they are all calling this method at the end.
        // Therefore, by wrapping this method with our lock, we make sure all get methods are safe to use.
        try {
            lock.readLock().lock();
            return super.get(path, def);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Set<String> getKeys(final boolean deep) {
        try {
            lock.readLock().lock();
            return super.getKeys(deep);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Map<String, Object> getValues(final boolean deep) {
        try {
            lock.readLock().lock();
            return super.getValues(deep);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void set(final String path, final Object value) {
        try {
            lock.writeLock().lock();
            super.set(path, value);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public String saveToString() {
        try {
            lock.readLock().lock();
            return super.saveToString();
        } finally {
            lock.readLock().unlock();
        }
    }

    private final class WriteRunner implements Runnable {

        private WriteRunner() {
        }

        @Override
        public void run() {
            //long startTime = System.nanoTime();
            synchronized (configFile) {
                try {
                    Files.createParentDirs(configFile);

                    if (!configFile.exists()) {
                        try {
                            LOGGER.log(Level.INFO, tl("creatingEmptyConfig", configFile.toString()));
                            if (!configFile.createNewFile()) {
                                LOGGER.log(Level.SEVERE, tl("failedToCreateConfig", configFile.toString()));
                                return;
                            }
                        } catch (final IOException ex) {
                            LOGGER.log(Level.SEVERE, tl("failedToCreateConfig", configFile.toString()), ex);
                            return;
                        }
                    }

                    try (final FileOutputStream fos = new FileOutputStream(configFile)) {
                        try (final OutputStreamWriter writer = new OutputStreamWriter(fos, UTF8)) {
                            writer.write(saveToString());
                        }
                    }
                } catch (final IOException e) {
                    LOGGER.log(Level.SEVERE, e.getMessage(), e);
                } finally {
                    //LOGGER.log(Level.INFO, configFile + " written to disk in " + (System.nanoTime() - startTime) + " nsec.");
                    writeTask = null;
                }
            }
        }
    }
}
